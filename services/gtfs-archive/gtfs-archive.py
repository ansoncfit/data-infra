import os
import sys
import time
import logging
import pathlib
import threading
import queue
import yaml
import urllib.request
import urllib.error

#class RhythmThread(threading.Thread):
#
#  def __init__(self, statusq, name):
#
#    super().__init__()
#
#    self.statusq = statusq
#    self.name    = name
#    self.logger  = None
#
#  def heartbeat(self):
#    try:
#      self.statusq.put_nowait({ 'thread': self.name, 'action': 'heartbeat' })
#    except queue.Full:
#      if not hasattr(self.logger, 'warning'):
#        self.logger = logging.getLogger()
#      self.logger.warning('{}: heartbeat dropped'.format(name))
#
#  def arrest(self):
#    try:
#      self.statusq.put_nowait({ 'thread': self.name, 'action': 'arrest' })
#    except queue.Full:
#      if not hasattr(self.logger, 'warning'):
#        self.logger = logging.getLogger()
#      self.logger.warning('{}: arrest dropped'.format(name))

class EventBus(object):

  def __init__(self, logger):

    self.lock = threading.Lock()
    self.listeners = {}
    self.logger = logger

  def add_listener(self, t_name, evt_name, evt_q):

    with self.lock.acquire():
      if evt_name in self.listeners:
        self.listeners[evt_name].add((t_name, evt_q))
      else:
        self.listeners[evt_name] = {(t_name, evt_q)}

  def rm_listener(self, t_name, evt_name, evt_q):

    with self.lock.acquire():
      if evt_name in self.listeners and (t_name, evt_q) in self.listeners[evt_name]:
        self.listeners[evt_name].remove((t_name, evt_q))
      else:
        return

  def emit(self, evt):

    evt_name = evt[0]

    with self.lock.acquire():
      for listener in self.listeners.get(evt_name, set()):
        t_name = listener[0]
        evt_q  = listener[1]
        try:
          evt_q.put_nowait(evt)
        except queue.Full:
          self.logger.warning('{}: event dropped: {}'.format(t_name, evt))

class Ticker(threading.Thread):

  def __init__(self, logger, evtbus, tickint):

    super().__init__()

    self.logger  = logger
    self.evtbus  = evtbus
    self.tickint = tickint
    self.name    = 'ticker'
    self.nticks  = 0

  def tick(self):
    evt = ( 'tick', self.nticks, time.time() )
    evtbus.emit(evt)
    self.logger.debug('{}: emit: {}'.format(self.name, evt))
    self.nticks += 1

  def run(self):

    self.nticks = 0
    self.tick()

    while time.sleep(self.tickint) is None:
      self.tick()

class Fetcher(threading.Thread):

  def __init__(self, logger, evtbus, wq, url, meta):

    super().__init__()

    self.logger = logger
    self.evtbus = evtbus
    self.wq   = wq
    self.url  = url
    self.meta = meta
    self.name = 'fetcher {}:{}'.format(meta['itp_id'], meta['gtfs_rt_url_name'])
    self.evtq = queue.Queue()

  def fetch(self):
    try:
      rs = urllib.request.urlopen(self.url)
      return rs
    except (
      urllib.error.URLError,
      urllib.error.HTTPError
    ) as e:
      self.logger.warning('{}: error fetching url {}: {}'.format(self.name, self.url, e))

  def run(self):

    self.evtbus.add_listener(self.name, 'tick', self.evtq)

    evt = self.evtq.get()
    while evt is not None:

      evt_name = evt[0]
      if evt_name == 'tick':
        rs = self.fetch()
        if hasattr(rs, 'read'):
          self.wq.put({
            'evt': evt,
            'data': rs,
            'meta': dict(self.meta)
          })

      evt = self.evtq.get()

    self.logger.debug('{}: finalized'.format(self.name))

if __name__ == '__main__':

  agencies_path = os.getenv('CALITP_AGENCIES_YML')
  tickint       = os.getenv('CALITP_TICK_INT')

  if agencies_path:
    agencies_path = pathlib.Path(agencies_path)
  else:
    agencies_path = pathlib.Path(os.getcwd(), 'agencies.yml')

  if tickint:
    tickint = float(tickint)
  else:
    tickint = 20.0
