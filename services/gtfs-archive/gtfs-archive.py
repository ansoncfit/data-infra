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

def main(argv):

  # Setup output channel

  logger = logging.getLogger(argv[0])
  logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

  # Parse environment

  agencies_path = os.getenv('CALITP_AGENCIES_YML')
  tickint       = os.getenv('CALITP_TICK_INT')

  if agencies_path:
    agencies_path = pathlib.Path(agencies_path)
  else:
    agencies_path = pathlib.Path(os.getcwd(), 'agencies.yml')

  if tickint:
    tickint = int(tickint)
  else:
    tickint = 20

  # Load data

  feeds    = parse_feeds(logger, agencies_path)

  # Instantiate threads

  wq       = queue.Queue()

  ## TESTING
  basepath = pathlib.Path('/tmp/gtfs-rt-data')
  ##

  evtbus  = EventBus(logger)
  ticker  = Ticker(logger, evtbus, tickint)
  writer  = FSWriter(logger, wq, basepath)

  fetchers = []
  for feed in feeds:
    fetchers.append(Fetcher(logger, evtbus, wq, feed))

  # Run

  writer.start()
  for fetcher in fetchers:
    fetcher.start()
  ticker.start()
  ticker.join()

#  feed = (
#    '279/trip_updates/0',
#    'http://api.bart.gov/gtfsrt/tripupdate.aspx'
#  )

def parse_feeds(logger, feeds_src):

  feeds = []

  with feeds_src.open() as f:

    feeds_src_data = yaml.load(f, Loader=yaml.FullLoader)
    for agency_name, agency_def in feeds_src_data.items():

      if 'gtfs_rt_urls' not in agency_def:
        continue

      if 'itp_id' not in agency_def:
        logger.warning('agency {}: skipped loading invalid definition (missing itp_id)'.format(agency_name))
        continue

      agency_itp_id       = agency_def['itp_id']
      agency_gtfs_rt_urls = agency_def['gtfs_rt_urls']

      if not hasattr(agency_gtfs_rt_urls, 'items'):
        logger.warning('agency {}/{}: skipped loading unsupported data format for gtfs_rt_urls'.format(agency_itp_id, agency_name))
        continue

      for agency_feed_name, agency_feed_urls in agency_gtfs_rt_urls.items():
        for i, url in enumerate(agency_feed_urls):
          feeds.append((
            '{}/{}/{}'.format(agency_itp_id, agency_feed_name, i),
            url
          ))

  return feeds


class EventBus(object):

  def __init__(self, logger):

    self.lock      = threading.Lock()
    self.listeners = {}
    self.logger    = logger

  def add_listener(self, t_name, evt_name, evt_q):

    with self.lock:
      if evt_name in self.listeners:
        self.listeners[evt_name].add((t_name, evt_q))
      else:
        self.listeners[evt_name] = {(t_name, evt_q)}

  def rm_listener(self, t_name, evt_name, evt_q):

    with self.lock:
      if evt_name in self.listeners and (t_name, evt_q) in self.listeners[evt_name]:
        self.listeners[evt_name].remove((t_name, evt_q))
      else:
        return

  def emit(self, evt):

    evt_name = evt[0]

    with self.lock:
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
    self.tickid  = 0

  def tick(self):
    evt = ( 'tick', self.tickid, int(time.time()) )
    self.logger.debug('{}: emit: {}'.format(self.name, evt))
    self.evtbus.emit(evt)
    self.tickid += 1

  def run(self):

    self.tickid = 0
    self.tick()

    while time.sleep(self.tickint) is None:
      self.tick()

class Fetcher(threading.Thread):

  def __init__(self, logger, evtbus, wq, urldef):

    super().__init__()

    self.logger  = logger
    self.evtbus  = evtbus
    self.wq      = wq
    self.urldef  = urldef
    self.name    = 'fetcher {}'.format(urldef[0])
    self.evtq    = queue.Queue()

  def fetch(self):
    url = self.urldef[1]
    try:
      rs = urllib.request.urlopen(url)
      return rs
    except (
      urllib.error.URLError,
      urllib.error.HTTPError
    ) as e:
      self.logger.info('{}: error fetching url {}: {}'.format(self.name, url, e))

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
            'urldef': self.urldef,
            'data': rs,
          })

      evt = self.evtq.get()

    self.logger.debug('{}: finalized'.format(self.name))

class FSWriter(threading.Thread):

  def __init__(self, logger, wq, basepath):

    super().__init__()

    self.logger   = logger
    self.wq       = wq
    self.basepath = basepath
    self.name     = 'fswriter'

  def write(self, item):

    evt_ts           = item['evt'][2]
    data_name        = item['urldef'][0]
    dest             = pathlib.Path(self.basepath, str(evt_ts), data_name)

    try:
      dest.parent.mkdir(parents=True, exist_ok=True)
    except OSError as e:
      self.logger.error('{}: mkdir: {}: {}'.format(self.name, dest.parent, e))
      return

    try:
      with dest.open(mode='wb') as f:
        f.write(item['data'].read())
    except OSError as e:
      self.logger.error('{}: write: {}: {}'.format(self.name, dest, e))
      return

  def run(self):

    item = self.wq.get()
    while item is not None:
      self.write(item)
      item = self.wq.get()

    self.logger.debug('{}: finalized'.format(self.name))

if __name__ == '__main__':
  main(sys.argv)
