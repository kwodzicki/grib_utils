import logging
import os
import functools
import time

from datetime import timedelta

from fasteners import InterProcessReaderWriterLock

class RateLimiter( object ):

  __LOCKFILE = os.path.join( '/tmp', os.path.basename(__file__)+'.lock' ) 

  def __init__(self, nping, units='minutes', **kwargs):
    self.__dt       = timedelta( **{units : 1.0 / nping} ).total_seconds()       # Get ping rate in seconds
    self.__lock     = InterProcessReaderWriterLock( self.__LOCKFILE )
    self.__log      = logging.getLogger(__name__)

  @staticmethod
  def limit(func):
    """Wait so that do not exceed rate limit"""

    @functools.wraps(func)
    def wrapped_limit(self, *args, **kwargs):
      with self.__lock.write_lock():                                            # Get the lock for thread safety!
        with open( self.__LOCKFILE, 'r+' ) as fid:
          try:
            lastPing = float( fid.read() )
          except:
            lastPing = 0.0
          dt       = self.__dt - (time.time() - lastPing)                        # Compute time since last download 
          if dt > 0.0:
            self.__log.debug( f'Rate limiting, sleep : {dt}' ) 
            time.sleep( dt )                                                # If time since last download is less than the minimum time allowed between downloads
          fid.seek(0)
          fid.write( str( time.time() ) )
          fid.truncate()
      return func( self, *args, **kwargs )

    return wrapped_limit
