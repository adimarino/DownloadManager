import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * Describes a file's metadata: URL, file name, size, and which parts already downloaded to disk.
 *
 * The metadata (or at least which parts already downloaded to disk) is constantly stored safely in disk.
 * When constructing a new metadata object, we first check the disk to load existing metadata.
 *
 * CHALLENGE: try to avoid metadata disk footprint of O(n) in the average case
 * HINT: avoid the obvious bitmap solution, and think about ranges...
 */
class DownloadableMetadata implements Serializable{
	
    private final String metadataFilename;
    private String filename;
    private String url;
    private long size;
    private long totalBytesWritten;
    private ArrayList<Range> downloaded;

    DownloadableMetadata(String url, long size) {
        this.url = url;
        this.filename = getName(url);
        this.metadataFilename = getMetadataName(filename);
        this.size = size;
        this.totalBytesWritten = 0;
        this.downloaded = new ArrayList<Range>();
    }

    private String getMetadataName(String filename) {
        return filename + ".metadata";
    }

    private static String getName(String path) {
        return path.substring(path.lastIndexOf('/') + 1, path.length());
    }

    /**
     * Add range to downloaded array.
     * @param range
     */
    void addRange(Range range) {
    	
    	this.totalBytesWritten += range.getLength();
    	
    	// In case no ranges added yet.
        if(this.downloaded.size() == 0){
        	this.downloaded.add(range);
        	return;
        }
    	
    	long start = range.getStart();
        long end = range.getEnd();
        
        // Add new range in correct place.
        for(int i = 0; i < this.downloaded.size(); i++){
        	Range current = this.downloaded.get(i);
        	
        	if(current.getStart() < start){
        		if(current.getEnd() >= start){
        			Range newRange = new Range(current.getStart(), end);
        			this.downloaded.add(i, newRange);
            		this.downloaded.remove(i + 1);
            		return;
        		}
        		if(i == this.downloaded.size() - 1){
        			this.downloaded.add(range);
        			return;
        		}
        	}
        	else{
        		if(current.getStart() <= end){
        			Range newRange = new Range(start, current.getEnd());
        			this.downloaded.add(i, newRange);
            		this.downloaded.remove(i + 1);
            		return;
        		}
        		else{ 
        			this.downloaded.add(i, range);
        			return;
        		}
        	}
        }
    }

    /**
     * Get file name.
     * @return file name.
     */
    public String getFilename() {
        return this.filename;
    }

    /**
     * Get metadata file name.
     * @return metadata file name.
     */
    public String getMetaDataFilename(){
    	return this.metadataFilename;
    }

    /**
     * Get total number of byte downloaded.
     * @return total number of byte downloaded
     */
    public long getTotalBytesWritten(){
    	return this.totalBytesWritten;
    }

    /**
     * Get file size.
     * @return file size.
     */
    public long getSize(){
    	return this.size;
    }

    /**
     * Check if the file download is completed.
     * @return true if download completed, otherwise returns flase.
     */
    boolean isCompleted() {
        Range range = this.getMissingRange();
        return range.getLength() == 1;
    }

    /**
     * Retrieve the first empty range located in Downloaded array list.
     * @return missing range.
     */
    public Range getMissingRange() {
        if(this.downloaded.size() == 0){
        	return new Range(0L, this.size);
        }
        
        for(int i = 0; i < this.downloaded.size(); i++){
        	Range current = this.downloaded.get(i);

        	// Case only one range in the array.
        	if(this.downloaded.size() == 1 && i == 0){
                if(current.getStart() != 0L){
                    return new Range(0L, current.getStart());
                }
                else{
                    return new Range(current.getEnd(), this.size);
                }
            }
        	Range next = this.downloaded.get(i + 1);
        	
        	// Case first range is not including position 0.
        	if(i == 0 && current.getStart() != 0L){
        		return new Range(0L, current.getStart());
        	} 
        	// Case last range is not including last position.
        	if(i == this.downloaded.size() - 1 && next.getEnd() != this.size -1){
        		//return new Range(next.getEnd() + 1, this.size - 1);
        		return new Range(current.getEnd(), this.size);
        	}
        	// Case of first missing range found. 
        	if(current.getEnd() < next.getStart() - 1){
        		return new Range(current.getEnd(), next.getStart());
        	} else {
                return new Range(next.getStart(), current.getEnd() -1);
            }
        }
        return null;
    }
}
