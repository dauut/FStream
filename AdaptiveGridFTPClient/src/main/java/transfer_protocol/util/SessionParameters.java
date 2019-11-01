package transfer_protocol.util;

public class SessionParameters {
    private int concurrency;
    private int parallelism;
    private int pipelining;
    private int bufferSize;
    private String chunkType;

    public String getChunkType() {
        return chunkType;
    }

    public void setChunkType(String chunkType) {
        this.chunkType = chunkType;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getPipelining() {
        return pipelining;
    }

    public void setPipelining(int pipelining) {
        this.pipelining = pipelining;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}
