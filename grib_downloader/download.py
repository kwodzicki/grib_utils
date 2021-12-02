import logging

import os
import re

from subprocess import Popen, STDOUT, PIPE, DEVNULL

from urllib.request import urlopen

from .ratelimit import RateLimiter
 
class GribDownloader( RateLimiter ):
  """Download full or partial grib files within server rate limit"""

  def __init__(self, *args, **kwargs):
    """
    Arguments:
      nping (int) : Number of pings per unit of time

    Keyword arguments:
      units (str) : Time units for the ping; i.e., nping per mintues

    """
 
    super().__init__(*args, **kwargs) 
    self.log = logging.getLogger( __name__ )
 
  def parseIDX( self, idxData, *args):
    """
    Parse IDX file for patterns of interest
  
    Use REGEX to find records in the IDX file that match the
    pattern(s) provided. If NO patterns found, then a None value
    is returned. Otherwise, the start/stop indices of all variables
    found are returned.
  
    Arguments:
      idxData (bytes,str) : Data from the idx file
      *args (str) : Any number of substrings to match records to
  
    Keyword arguments:
      None.
  
    Returns:
      list : List of strings containing ranges of bytes offsets into grib file
  
    """
  
    if isinstance(idxData, bytes): idxData = idxData.decode()                   # If the idxData in bytes, decode to string
    pattern = "^.*(?:{}).*$".format( '|'.join( args ) )                         # Generate regex pattern to search for
    matches = re.findall( pattern, idxData, re.MULTILINE )                      # Search the idx data for the patterns
    if len(matches) > 0:                                                        # If at least one (1) match found
      if len(matches) != len(args):
        self.log.debug('Missing some variables!')                               # If not all matched; warning
      records = idxData.splitlines()                                            # Split records by line
      ranges  = [''] * len(matches)                                             # Initialize list of strings for matches
      for i, match in enumerate( matches ):                                     # Iterate over matches
        index     = records.index( match )
        offset    = match.split(':')[1]
        ranges[i] = offset                                                      # Store the offset in ranges at index i
        try:                                                                    # Try to
          nextRec = records[ index+1 ]                                          # Get the next record; remember record indices start at one (1), whereas python indexes starting at zero (0), so no need to add one to get next record
        except:                                                                 # On exception
          pass                                                                  # We are at end of records, so we don't need to do anything
        else:                                                                   # Else, we got a next record
          index      = records.index( nextRec )
          offset     = nextRec.split(':')[1]                                    # Get the index and offset of the next record
          ranges[i] += '-' + str( int(offset)-1 )                               # Add and end point to the range at index i; remember that the offset is the start of the next record, so we need to decrement by one (1) to get end of record of interest
      self.log.debug( 'Will grab data in range: {}'.format( ranges[i] ) )
      return ranges                                                             # Return the list of ranges
  
    self.log.error('No variables found matching pattern' )                      # If made here, print warning

    return None                                                                 # Return None

  @RateLimiter.limit
  def downloadIDX( self, remote, local = None, clobber=False, **kwargs ):
    """
    Download IDX file/data

    Can download IDX file data and save to a file given local

    Arguments:
      remote (str) : Remote server path for file

    Keyword arguments:
      local (str) : Local path to save IDX data to. Default is to not save
        data to file on local disk 
      clobber (bool) : If set, will redownload/overwrite any existing local
        IDX file
      kwargs : Silently ignores any other keywords

    Returns:
      bytes : Data from IDX file as bytes

    """
 
    if (not clobber) and (local is not None) and os.path.isfile(local):         # If clobber not set and the local file exists
      self.log.info( 'Local file exists, reading in existing; set clobber=True to overwrite' )
      with open(local, 'rb') as fid:                                            # Open local file for reading in binary
        return fid.read()                                                       # Read in binary data and return
  
    self.log.debug( 'Trying to open remote url' )
    try:
      res = urlopen( remote )
    except:
      self.log.error( 'Failed to open remote: {}'.format( remote ) )
      return None
  
    self.log.debug( 'Trying to read remote url' )
    try:
      data = res.read()
    except:
      self.log.error( 'Failed to read data: {}'.format( remote ) )
      return None
  
    self.log.debug( 'Trying to close remote url' )
    try:
      res.close()
    except:
      pass
  
    if local is not None:                                                       # If local is set
      self.log.info( f'Writing data to local file : {local}' )
      ldir = os.path.dirname( local )                                           # Get directory name
      if not os.path.isdir( ldir ):                                             # If directory not exist
        os.makedirs( os.path.dirname( ldir ) )                                  # Create directory
      with open( local, 'wb' ) as fid:                                          # Open loacl file for writing
        fid.write( data )                                                       # Write data to file
   
    return data                                                                 # Return data

  @RateLimiter.limit
  def downloadGrib(self, url, outfile, offsets = None):
    """
    Download grib file from url to outfile

    Arguments:
      url (str) : Remote path to grib file
      outfile (str) : Local path to download data to

    Keyword arguments:
      offsets (tuple) : List of offsets into grib file to download;
        See the parseIDX method

    Returns:
      None.

    """

    cmd     = ['curl', '-f', '-v', '-s']                                    # Base curl command
    if offsets:
      cmd.extend( ['-r', ','.join(offsets)] )

    if not os.path.isdir( os.path.dirname( outfile ) ):                     # If output file directory NOT exist
      os.makedirs( os.path.dirname( outfile ) )                             # Create it
    elif os.path.isfile( outfile ):                                         # else, if the output file exists
      os.remove( outfile )                                                  # Remove it
    cmd.extend( [url, '-o', outfile] )                                      # Extend the download command to run with the remote URL and local file path

    self.log.debug( ' '.join(cmd) )                                         # Log the command to be run

    proc = Popen( cmd, stdout=PIPE, stderr=STDOUT, universal_newlines=True) # Spawn the download
    line = proc.stdout.readline()                                           # Read line for subprocess
    while line != '':                                                       # While not an empty line
      self.log.log( 5, line.rstrip() )                                      # Log the line to very low level
      line = proc.stdout.readline()                                         # Read another line

    proc.communicate()

    return proc.returncode == 0

  def getGrib( self, url, outfile, *args, clobber = False, **kwargs):
    """
    Download a grib file

    Arguments:
      url (str) : URL of remote file to download
      outfile (str) : Local path to download file to
      *args (str) : Any number of substrings to match records to

    Keyword arguments:
      clobber (bool) : Set to overwrite/redownload existing files.
      callback (func) : Function to run after file downloaded. Function
        must accept one (1) input, which is the output file path
      no_idx (bool) : If set, no IDX file will be stored on local disk
      kwargs : 

    """

    callback  = kwargs.get('callback', None)                                    # Get callback function from arguments

    if os.path.isfile( outfile ) and not clobber:                               # If file exists
      self.log.info( f'File already exists : {url} ---> {outfile}' )
    else:                                                                       # Else
      self.log.info( f'Attempting to download : {url}' )

      idxRemote = url + '.idx'                                                  # Remote file path
      if kwargs.get('no_idx', False):                                           # If no_idx key is set
        idxLocal  = None
      else:                                                                     # Else
        outdir   = os.path.dirname( outfile )                                   # Get output directory from outfile path
        idxLocal = os.path.join( outdir, idxRemote.split('/')[-1] )             # Build local IDX file path

      #self.wait()                                                               # Wait to ensure we are not hitting the rate limit
      idxData = self.downloadIDX( idxRemote, idxLocal, **kwargs )               # Try to download the idx file
      if idxData is None:                                                       # If no data downloaded
        self.log.error( 'IDX file not found on remote server: {}'.format( idxRemote ) )
        return 0
      else:
        offsets = self.parseIDX( idxData, *args )                               # Get file offsets for downloading only variables of interest
        print( offsets )
        if not self.downloadGrib( url, outfile, offsets = offsets ):
          self.log.warning( f'Error with cURL command while downloading : {url}' )

    check = os.path.isfile( outfile )
    if (callback is not None) and check:
      callback( outfile )                                                       # If callback is set, then run callback command
    else:
      return check                                                              # Return True/False for if file exists
