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
        		}
        		else{ 
        			this.downloaded.add(i, range);
        		}
        	}
        }
    }

    public String getFilename() {
        return this.filename;
    }
    
    public String getMetaDataFilename(){
    	return this.metadataFilename;
    }
    
    public long getTotalBytesWritten(){
    	return this.totalBytesWritten;
    }

    public long getSize(){
    	return this.size;
    }
    
    boolean isCompleted() {
    	System.out.println("Array size:" + downloaded.size() + " downloaded size cond returns: " + (downloaded.size() == 1));
    	System.out.println("Range 0: start point - " + downloaded.get(0).getStart() + " end point - " + downloaded.get(0).getEnd());
    	System.out.println("File size: " + this.size);
        return (downloaded.size() == 1) && (downloaded.get(0).getStart() == 0L) && (downloaded.get(0).getEnd() == this.size) && (this.getMissingRange() == null);
    }

    void delete() {
        File metaDataToDelete = new File(this.metadataFilename);
        metaDataToDelete.delete();
    }

    Range getMissingRange() {
        if(this.downloaded.size() == 0){
        	System.out.println("all" + 0 +"-"+this.size);
        	return new Range(0L, this.size);
        }
        
        for(int i = 0; i < this.downloaded.size() - 1; i++){
        	Range current = this.downloaded.get(i);
        	Range next = this.downloaded.get(i + 1);
        	
        	// Case first range is not including position 0.
        	if(i == 0 && current.getStart() != 0L){
        		System.out.println("1: " + 0 +"-"+current.getStart());
        		return new Range(0L, current.getStart());
        	} 
        	// Case last range is not including last position.
        	if(i == this.downloaded.size() - 1 && next.getEnd() != this.size -1){
        		//return new Range(next.getEnd() + 1, this.size - 1);
        		System.out.println("2 " + current.getEnd() +"-"+this.size);
        		return new Range(current.getEnd(), this.size);
        	}
        	// Case of first missing range found. 
        	if(current.getEnd() != next.getStart() - 1){
        		System.out.println("3 " + current.getEnd() +"-"+next.getStart());
        		return new Range(current.getEnd(), next.getStart());
        	}
        }
        return null;
    }

    String getUrl() {
        return url;
    }
}
