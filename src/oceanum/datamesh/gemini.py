import time
import io

class UploadStalledError(Exception):
    """Custom exception for upload stalls."""
    pass

class MonitoredStreamingBody:
    def __init__(self, source, stall_timeout=30):
        """
        Args:
            source: A file-like object (e.g., open file handle) or a generator.
            stall_timeout: Max seconds allowed between reads before raising UploadStalledError.
        """
        self._source = source
        self._stall_timeout = stall_timeout
        self._last_read_time = time.monotonic()
        self._len = None

        # Try to determine content length if possible (important!)
        if hasattr(source, 'seek') and hasattr(source, 'tell'):
            try:
                current_pos = source.tell()
                source.seek(0, os.SEEK_END)
                self._len = source.tell()
                source.seek(current_pos) # Reset position
            except (io.UnsupportedOperation, OSError):
                self._len = None # Cannot determine length
        elif hasattr(source, '__len__'):
             # Basic support for sources that might know their len
            try:
                self._len = len(source)
            except TypeError:
                self._len = None

    def __len__(self):
        if self._len is not None:
            return self._len
        raise TypeError("Object of type MonitoredStreamingBody has no len()")

    def read(self, size=-1):
        """Reads from the source, monitoring for stalls."""
        now = time.monotonic()
        if now - self._last_read_time > self._stall_timeout:
            raise UploadStalledError(
                f"Upload stalled: No data read for more than {self._stall_timeout} seconds."
            )

        # Read from the underlying source
        # Note: Generators don't take 'size', file objects do.
        # This simplified version assumes a file-like object primarily.
        # For a generator source, you'd adapt this logic (likely using next()).
        try:
            chunk = self._source.read(size)
        except Exception as e:
            # Handle potential errors during read from source
            print(f"Error reading from source: {e}")
            raise # Re-raise the original error

        self._last_read_time = time.monotonic() # Update time *after* successful read attempt
        return chunk


class MonitoredStreamingBody:
    def __init__(self, source, stall_timeout=30):
        self._source = source
        self._stall_timeout = stall_timeout
        self._last_read_time = time.monotonic()
        self._len = None

        if hasattr(source, 'seek') and hasattr(source, 'tell'):
            try:
                current_pos = source.tell()
                source.seek(0, os.SEEK_END)
                self._len = source.tell()
                source.seek(current_pos)
                print(f"MonitoredStreamingBody: Determined length = {self._len}") # Debugging
            except (io.UnsupportedOperation, OSError):
                self._len = None
        elif hasattr(source, '__len__'):
            try:
                self._len = len(source)
                print(f"MonitoredStreamingBody: Determined length via __len__ = {self._len}") # Debugging
            except TypeError:
                self._len = None

        if self._len is None:
             print("MonitoredStreamingBody: Could not determine length.") # Debugging


    def __len__(self):
        if self._len is not None:
            return self._len
        raise TypeError("Object of type MonitoredStreamingBody has no len()")

    def read(self, size=-1):
        now = time.monotonic()
        elapsed_since_last_read = now - self._last_read_time
        # Optional: Add some logging to see timings
        # print(f"MonitoredStreamingBody.read({size}) called. Time since last read: {elapsed_since_last_read:.2f}s")

        if elapsed_since_last_read > self._stall_timeout:
            raise UploadStalledError(
                f"Upload stalled: No data read for more than {self._stall_timeout} seconds "
                f"(elapsed: {elapsed_since_last_read:.2f}s)."
            )
        try:
            chunk = self._source.read(size)
        except Exception as e:
            print(f"Error reading from source: {e}")
            raise

        # Update time *after* successful read attempt, even if chunk is empty (EOF)
        self._last_read_time = time.monotonic()
        # if not chunk:
        #    print("MonitoredStreamingBody.read() returning EOF (empty bytes)")
        return chunk

# Timeout Configuration
CONNECT_TIMEOUT = 10  # Seconds to establish connection
READ_TIMEOUT = 300    # Seconds to wait for server RESPONSE after upload finishes
STALL_TIMEOUT = 45    # Seconds of inactivity during UPLOAD before considering it stalled

# Retry Configuration
MAX_RETRIES = 5
INITIAL_BACKOFF = 2 # Seconds


def upload_with_retries(filepath, url):
    last_exception = None
    for attempt in range(MAX_RETRIES):
        try:
            with open(filepath, 'rb') as f:
                # Crucial: Create a *new* monitored body for each attempt,
                # as the file position needs to be reset.
                monitored_body = MonitoredStreamingBody(f, stall_timeout=STALL_TIMEOUT)

                headers = {}
                # Add Content-Length if known, otherwise requests uses chunked encoding
                if hasattr(monitored_body, '__len__'):
                    headers['Content-Length'] = str(len(monitored_body))
                else:
                    # Let requests handle chunked transfer encoding
                    pass

                print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Uploading {filepath}...")

                response = requests.post(
                    url,
                    data=monitored_body,
                    headers=headers,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
                )

                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

                print(f"Upload successful on attempt {attempt + 1}!")
                return response # Or return True, or response content, etc.

        except UploadStalledError as e:
            print(f"Attempt {attempt + 1} failed: Upload stalled. {e}")
            last_exception = e
        except requests.exceptions.Timeout as e:
            # Catches both ConnectTimeout and ReadTimeout
            print(f"Attempt {attempt + 1} failed: Request timed out. {e}")
            last_exception = e
        except requests.exceptions.ConnectionError as e:
            print(f"Attempt {attempt + 1} failed: Connection error. {e}")
            last_exception = e
        except requests.exceptions.HTTPError as e:
            # Handle HTTP errors (e.g., 5xx server errors might be retryable)
            if 500 <= e.response.status_code < 600:
                print(f"Attempt {attempt + 1} failed: Server error ({e.response.status_code}). {e}")
                last_exception = e
            else:
                # Don't retry client errors (4xx) by default
                print(f"Attempt {attempt + 1} failed: HTTP error ({e.response.status_code}). Not retrying. {e}")
                raise e # Re-raise non-retryable HTTP errors
        except IOError as e:
             # Catch potential file reading errors
             print(f"Attempt {attempt + 1} failed: File IO error. {e}")
             raise e # Likely not retryable if the file is bad
        except Exception as e:
            # Catch any other unexpected errors
            print(f"Attempt {attempt + 1} failed: An unexpected error occurred. {e}")
            last_exception = e
            # Consider whether to retry unexpected errors

        # If we got here, an exception occurred and we might retry
        if attempt < MAX_RETRIES - 1:
            backoff_time = INITIAL_BACKOFF * (2 ** attempt)
            print(f"Retrying in {backoff_time:.2f} seconds...")
            time.sleep(backoff_time)
        else:
            print("Maximum retries reached. Upload failed.")
            if last_exception:
                raise last_exception # Re-raise the last captured exception
            else:
                 # Should not happen if loop completed, but good practice
                 raise Exception("Upload failed after maximum retries for unknown reasons.")


# Assume 'large_in_memory_data' is your bytes object (potentially several GB)
# Example: large_in_memory_data = b'...' * 1024 * 1024 * 500 # 500MB example

url = "your_api_endpoint"

# Timeout Configuration (Revised based on your observation)
# Treat connect timeout as potentially covering the *entire write phase*
# Set based on max expected upload time for your largest in-memory object + buffer
CONNECT_TIMEOUT = 600 # Seconds (e.g., 10 minutes - adjust based on size/network)

# Read timeout covers server processing *after* upload completes
READ_TIMEOUT = 300    # Seconds (e.g., 5 minutes - adjust based on server needs)

# Stall timeout detects pauses *during* the upload via the wrapper
STALL_TIMEOUT = 45    # Seconds

# Retry Configuration
MAX_RETRIES = 5
INITIAL_BACKOFF = 2 # Seconds

def upload_in_memory_with_retries(data_bytes, url):
    """Uploads in-memory bytes with monitoring and retries."""
    last_exception = None
    data_len = len(data_bytes) # Get length once

    for attempt in range(MAX_RETRIES):
        try:
            # Create a *new* BytesIO and Monitored body for each attempt
            bytes_io_source = io.BytesIO(data_bytes)
            monitored_body = MonitoredStreamingBody(bytes_io_source, stall_timeout=STALL_TIMEOUT)

            headers = {
                # Content-Length will be correctly set by requests automatically
                # because MonitoredStreamingBody implements __len__ using BytesIO's info.
                'Content-Length': str(data_len)
            }

            print(f"Attempt {attempt + 1}/{MAX_RETRIES}: Uploading {data_len} bytes...")

            response = requests.post(
                url,
                data=monitored_body,
                headers=headers,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
            )

            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

            print(f"Upload successful on attempt {attempt + 1}!")
            return response

        except UploadStalledError as e:
            print(f"Attempt {attempt + 1} failed: Upload stalled. {e}")
            last_exception = e
        except requests.exceptions.Timeout as e:
            # Based on your observation, this might trigger if the *write* takes
            # longer than CONNECT_TIMEOUT, or if waiting for the response
            # takes longer than READ_TIMEOUT.
            print(f"Attempt {attempt + 1} failed: Request timed out (Connect/Read/PotentialWrite). {e}")
            last_exception = e
        except requests.exceptions.ConnectionError as e:
            print(f"Attempt {attempt + 1} failed: Connection error. {e}")
            last_exception = e
        except requests.exceptions.HTTPError as e:
            if 500 <= e.response.status_code < 600:
                print(f"Attempt {attempt + 1} failed: Server error ({e.response.status_code}). {e}")
                last_exception = e
            else:
                print(f"Attempt {attempt + 1} failed: HTTP error ({e.response.status_code}). Not retrying. {e}")
                raise e
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: An unexpected error occurred. {e}")
            last_exception = e
            # Decide if unexpected errors are retryable

        # --- Backoff logic ---
        if attempt < MAX_RETRIES - 1:
            backoff_time = INITIAL_BACKOFF * (2 ** attempt)
            print(f"Retrying in {backoff_time:.2f} seconds...")
            time.sleep(backoff_time)
        else:
            print("Maximum retries reached. Upload failed.")
            if last_exception:
                raise last_exception
            else:
                 raise Exception("Upload failed after maximum retries for unknown reasons.")


# --- Example Usage ---
# Create some large dummy data
print("Creating dummy data...")
try:
    # Adjust size as needed for testing
    # data_size_gb = 1
    # large_in_memory_data = b'\0' * (data_size_gb * 1024 * 1024 * 1024)

    # Use a smaller size for quicker testing initially
    data_size_mb = 200
    large_in_memory_data = b'\x42' * (data_size_mb * 1024 * 1024)
    print(f"Dummy data size: {len(large_in_memory_data) / (1024*1024):.2f} MB")
except MemoryError:
    print("Failed to allocate large memory block for testing. Reduce size.")
    large_in_memory_data = None

if large_in_memory_data:
    try:
        print("Starting upload...")
        upload_response = upload_in_memory_with_retries(large_in_memory_data, url)
        if upload_response:
            print("Final Status Code:", upload_response.status_code)
            # Process response
    except Exception as e:
        print(f"Upload ultimately failed: {type(e).__name__}: {e}")
        # Final error handling

        