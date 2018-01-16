import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.BlockingQueue;

/**
 * This class takes chunks from the queue, writes them to disk and updates the file's metadata.
 *
 * NOTE: make sure that the file interface you choose writes every update to the file's content or metadata
 *       synchronously to the underlying storage device.
 */
public class FileWriter implements Runnable {

    private final BlockingQueue<Chunk> chunkQueue;
    private DownloadableMetadata downloadableMetadata;
    private boolean isTerminated;

    FileWriter(DownloadableMetadata downloadableMetadata, BlockingQueue<Chunk> chunkQueue) {
        this.chunkQueue = chunkQueue;
        this.downloadableMetadata = downloadableMetadata;
        this.isTerminated = false;
    }

	/**
	 * Write data chunks to file.
	 * @throws IOException
	 */
	private void writeChunks() throws IOException {
    	// Create new download file if needed.
    	File file = new File(this.downloadableMetadata.getFilename());
    	if (!file.exists()){
        	file.createNewFile();
        }
    	RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");

    	long fileSize = this.downloadableMetadata.getSize();
    	long downloaded = this.downloadableMetadata.getTotalBytesWritten();
    	int percent = (int) (((double) downloaded / fileSize) * 100);
    	System.err.println("Downloaded " + percent + "%");
    	
    	// Take chunks until queueu is empty, and write them to dowloaded file and metadata.
    	try {
    		while(true){
    		
    			Chunk chunk = this.chunkQueue.take();

    			// Check if filewriter is terminated.
    			if(chunk.getOffset() == -1){
    				break;
    			}
				// Seek correct position for writing to downloaded file.
				randomAccessFile.seek(chunk.getOffset());
				// Write to dowonloaded file.
				randomAccessFile.write(chunk.getData(), 0, (int)chunk.getSizeInBytes());

				Range range = new Range(chunk.getOffset(), chunk.getOffset() + chunk.getSizeInBytes());
				this.downloadableMetadata.addRange(range);
				
				// Print current download percentage.
				downloaded = this.downloadableMetadata.getTotalBytesWritten();
				int currentPercent = (int) (((double) downloaded / fileSize) * 100);
				if(currentPercent != percent){
					System.err.println("Downloaded " + currentPercent + "%");
					percent = currentPercent;
				}

				FileOutputStream metaDataFile = null;
				ObjectOutputStream metadataStream = null;
				try {
					metaDataFile = new FileOutputStream(this.downloadableMetadata.getMetaDataFilename());
					metadataStream = new ObjectOutputStream(metaDataFile);
					metadataStream.writeObject(downloadableMetadata);
				} catch (Exception e1){
					metadataStream.close();
					metaDataFile.close();
				}
    		}
    	} catch (InterruptedException e) {
			e.printStackTrace();
			System.err.println("Download failed");
		} finally {
			randomAccessFile.close();
		}
    }
    
    @Override
    public void run() {
        try {
            this.writeChunks();
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Download failed");
        }
    }
}
