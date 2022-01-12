import logging

from functools import partial

class Partial( partial ):

  def __call__(self, /, *args, **keywords):
    keywords = {**self.keywords, **keywords}
    return self.func(*args, *self.args, **keywords)

def parseIDX( idxData, *args):
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

  log = logging.getLogger(__name__)
  if isinstance(idxData, bytes): idxData = idxData.decode()                   # If the idxData in bytes, decode to string
  pattern = "^.*(?:{}).*$".format( '|'.join( args ) )                         # Generate regex pattern to search for
  matches = re.findall( pattern, idxData, re.MULTILINE )                      # Search the idx data for the patterns
  if len(matches) > 0:                                                        # If at least one (1) match found
    if len(matches) != len(args):
      log.debug('Missing some variables!')                                    # If not all matched; warning
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
    log.debug( 'Will grab data in range: {}'.format( ranges[i] ) )
    return ranges                                                             # Return the list of ranges

  log.error('No variables found matching pattern' )                           # If made here, print warning

  return None                                                                 # Return None

def subsetGrib( gribFile, *args, remove_idx = False ):

  if isinstance( gribFile, (tuple, list) ):
    func = Partial( subsetGrib, *args, remove_idx = remove_idx ) 
    return tuple( map( func, gribFile ) )

  idxFile = f'{gribFile}.idx'
  if not os.path.isfile( idxFile ):
    log.error( 'No IDX file found : {idxFile}' )
    return False

  with open( idxFile, 'r' ) as fid:
    offsets = parseIDX( fid.read(), *args )
  if offsets:
    data = b''
    with open( gribFile, 'rb' ) as fid:
      for offset in offsets:
        sid, eid = map(int, offset.split('-'))
        fid.seek(sid, 0)
        data += fid.read( eid-sid+1 )
    with open( gribFile, 'wb' ) as fid:
      fid.write( data )

  if remove_idx: os.remove( idxFile )

  return True
