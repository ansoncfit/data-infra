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

class RhythmThread(threading.Thread):

  def __init__(self, statusq, name):

    super().__init__()

    self.statusq = statusq
    self.name    = name
    self.logger  = None

  def heartbeat(self):
    try:
      self.statusq.put_nowait({ 'thread': self.name, 'action': 'heartbeat' })
    except queue.Full:
      if not hasattr(self.logger, 'warning'):
        self.logger = logging.getLogger()
      self.logger.warning('{}: heartbeat dropped'.format(name))

  def arrest(self):
    try:
      self.statusq.put_nowait({ 'thread': self.name, 'action': 'arrest' })
    except queue.Full:
      if not hasattr(self.logger, 'warning'):
        self.logger = logging.getLogger()
      self.logger.warning('{}: arrest dropped'.format(name))

class Ticker(threading.Thread):

  def __init__(self, tickint):

    super().__init__()

    self.name    = 'ticker'
    self.tickint = tickint
    self.subs    = []

  def add_subscriber(self, name, evtq):
    self.subs.append({'name': name, 'q': evtq})

  def run(self):

    self.logger = logging.getLogger()

    while time.sleep(self.tickint) is None:
      evt = 'tick:{}'.format(time.time())
      self.logger.debug('event: {}'.format(evt))
      for sub in self.subs:
        try:
          sub['q'].put_nowait(evt)
        except queue.Full:
          self.logger.warning('{}: dropped event {}'.format(sub['name'], evt))
          continue

class Fetcher(threading.Thread):

  def __init__(self, evtq, wq, url, meta):

    super().__init__()

    self.name = 'fetcher {}:{}'.format(meta['itp_id'], meta['gtfs_rt_url_name'])
    self.evtq = evtq
    self.wq   = wq
    self.url  = url
    self.meta = meta

  def run(self):

    self.logger = logging.getLogger()
    self.logger.debug('{}: started'.format(self.name))

    evt = self.evtq.get()
    while evt is not None:
      try:
        rs = urllib.request.urlopen(self.url)
        self.wq.put({
          'evt': evt,
          'data': rs.read(),
          'meta': dict(self.meta)
        })
      except (
        urllib.error.URLError,
        urllib.error.HTTPError
      ) as e:
        self.logger.warning('{}: error fetching url {}: {}'.format(self.name, self.url, e))
      evt = self.evtq.get()

    self.logger.debug('{}: finalized'.format(self.name)

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
