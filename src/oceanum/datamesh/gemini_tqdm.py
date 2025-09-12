import time
import io
import os # Keep os for SEEK_END etc.

# --- UploadStalledError definition remains the same ---
class UploadStalledError(Exception):
    """Custom exception for upload stalls."""
    pass

class MonitoredStreamingBody:
    def __init__(self, source, stall_timeout=30, callback=None): # Add callback
        """
        Args:
            source: A file-like object (e.g., open file handle, BytesIO).
            stall_timeout: Max seconds allowed between reads before raising UploadStalledError.
            callback: Optional function to call with (bytes_uploaded, total_bytes).
        """
        self._source = source
        self._stall_timeout = stall_timeout
        self._callback = callback # Store the callback
        self._last_read_time = time.monotonic()
        self._len = None
        self._uploaded = 0 # Track bytes uploaded through this instance

        # Determine content length (same logic as before)
        if hasattr(source, 'seek') and hasattr(source, 'tell'):
            try:
                current_pos = source.tell()
                source.seek(0, os.SEEK_END)
                self._len = source.tell()
                source.seek(current_pos)
            except (io.UnsupportedOperation, OSError):
                self._len = None
        elif hasattr(source, '__len__'):
            try:
                self._len = len(source)
            except TypeError:
                self._len = None

    def __len__(self):
        if self._len is not None:
            return self._len
        raise TypeError("Object of type MonitoredStreamingBody has no len()")

    def read(self, size=-1):
        now = time.monotonic()
        elapsed_since_last_read = now - self._last_read_time

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

        self._last_read_time = time.monotonic()

        # --- Progress Bar Logic ---
        if chunk: # Only update if data was actually read
            chunk_size = len(chunk)
            self._uploaded += chunk_size
            if self._callback:
                try:
                    # Call the callback with cumulative uploaded and total size
                    self._callback(self._uploaded, self._len if self._len is not None else 0)
                except Exception as cb_exc:
                    # Catch errors in the callback to avoid disrupting the upload
                    print(f"Warning: Progress callback failed: {cb_exc}")
        # --- End Progress Bar Logic ---

        return chunk

import requests
import time
import io
from tqdm import tqdm # Import tqdm

# --- MonitoredStreamingBody and UploadStalledError definitions here ---
# (Use the modified version from step 1)

# --- Timeout/Retry Configuration remains the same ---
CONNECT_TIMEOUT = 600
READ_TIMEOUT = 300
STALL_TIMEOUT = 45
MAX_RETRIES = 5
INITIAL_BACKOFF = 2

def upload_in_memory_with_retries(data_bytes, url):
    """Uploads in-memory bytes with monitoring, retries, and progress bar."""
    last_exception = None
    data_len = len(data_bytes)

    # Create the tqdm progress bar *outside* the retry loop
    # We'll update/reset it inside the loop
    with tqdm(total=data_len, unit='B', unit_scale=True, desc="Uploading", leave=False) as pbar:
        # Define the callback function (closure) that updates the progress bar
        def progress_callback(uploaded, total):
            # Update the bar to the current cumulative value
            pbar.n = uploaded
            pbar.refresh() # Ensure display updates

        for attempt in range(MAX_RETRIES):
            try:
                # Reset progress bar for the new attempt
                pbar.reset(total=data_len)
                pbar.set_description(f"Uploading (Attempt {attempt + 1}/{MAX_RETRIES})")

                bytes_io_source = io.BytesIO(data_bytes)

                # Pass the callback here
                monitored_body = MonitoredStreamingBody(
                    bytes_io_source,
                    stall_timeout=STALL_TIMEOUT,
                    callback=progress_callback # Pass the callback function
                )

                headers = {'Content-Length': str(data_len)}

                print(f"\nAttempt {attempt + 1}/{MAX_RETRIES}: Uploading {data_len} bytes...") # Keep console log

                response = requests.post(
                    url,
                    data=monitored_body,
                    headers=headers,
                    timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
                )

                # Ensure bar reaches 100% on success if upload was smaller than total?
                # This shouldn't be needed if callback is called correctly on last chunk.
                # pbar.n = data_len
                # pbar.refresh()

                response.raise_for_status()

                pbar.set_description("Upload successful!")
                pbar.leave = True # Keep the completed bar visible after the 'with' block
                print(f"\nUpload successful on attempt {attempt + 1}!")
                return response

            # --- Exception handling remains the same ---
            except UploadStalledError as e:
                pbar.set_description(f"Stalled (Attempt {attempt+1})")
                print(f"\nAttempt {attempt + 1} failed: Upload stalled. {e}")
                last_exception = e
            except requests.exceptions.Timeout as e:
                pbar.set_description(f"Timeout (Attempt {attempt+1})")
                print(f"\nAttempt {attempt + 1} failed: Request timed out. {e}")
                last_exception = e
            except requests.exceptions.ConnectionError as e:
                pbar.set_description(f"Connection Error (Attempt {attempt+1})")
                print(f"\nAttempt {attempt + 1} failed: Connection error. {e}")
                last_exception = e
            except requests.exceptions.HTTPError as e:
                pbar.set_description(f"HTTP Error {e.response.status_code} (Attempt {attempt+1})")
                if 500 <= e.response.status_code < 600:
                    print(f"\nAttempt {attempt + 1} failed: Server error ({e.response.status_code}). {e}")
                    last_exception = e
                else:
                    print(f"\nAttempt {attempt + 1} failed: HTTP error ({e.response.status_code}). Not retrying. {e}")
                    pbar.leave = True # Keep error bar visible
                    raise e
            except Exception as e:
                pbar.set_description(f"Error (Attempt {attempt+1})")
                print(f"\nAttempt {attempt + 1} failed: An unexpected error occurred. {e}")
                last_exception = e
                # Potentially raise immediately for unexpected errors
                # raise e

            # --- Backoff logic remains the same ---
            if attempt < MAX_RETRIES - 1:
                backoff_time = INITIAL_BACKOFF * (2 ** attempt)
                print(f"Retrying in {backoff_time:.2f} seconds...")
                # Display backoff in tqdm description?
                pbar.set_description(f"Retrying in {backoff_time:.0f}s (Attempt {attempt + 2})")
                # Note: tqdm doesn't have a built-in sleep indicator,
                # the bar will just pause here.
                time.sleep(backoff_time)
            else:
                pbar.set_description("Failed after max retries")
                pbar.leave = True # Keep failed bar visible
                print("\nMaximum retries reached. Upload failed.")
                if last_exception:
                    raise last_exception
                else:
                    raise Exception("Upload failed after maximum retries for unknown reasons.")

# --- Example Usage (same as before) ---
# ... create large_in_memory_data ...
# ... call upload_in_memory_with_retries ...
# ... handle final success/failure ...