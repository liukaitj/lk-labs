package lk.lab.maxcompute.oss.split;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import com.aliyun.odps.io.SourceInputStream;

/**
 * A reader that reads files split, it encapsulates an internal Reader, and
 * wraps around the read operation with a sanity check to safeguard against
 * reading pass the split end. Note that with this reader the split boundaries
 * provided must be at exact record boundaries.
 */
class SplitReader extends Reader {

    protected InputStreamReader  internalReader;
    protected long    splitSize;
    protected long    splitReadLen;
    protected Charset charset;

    protected SplitReader() {
    }

    public SplitReader(SourceInputStream stream, Charset charset) throws IOException {
        this.charset = charset;
        this.splitReadLen = 0;
        long currentPos = stream.getCurrentPos();
        long splitStart = stream.getSplitStart();
        this.splitSize = stream.getSplitSize();
        if (currentPos < splitStart) {
            System.out.println("Skipping: " + (splitStart - currentPos) + " bytes to split start.");
            stream.skip(splitStart - currentPos);
            currentPos = stream.getCurrentPos();
        }
        System.out.println("Processing bytes [" + currentPos + " , " + (currentPos + splitSize - 1)
                + "] for file " + stream.getFileName());
        this.internalReader = new InputStreamReader(stream, charset);
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        if (this.splitReadLen >= this.splitSize) {
            return -1;
        }
        if (this.splitReadLen + len >= this.splitSize) {
            len = (int) (this.splitSize - this.splitReadLen);
        }
        int readSize = this.internalReader.read(cbuf, off, len);
//        this.splitReadLen += readSize;
        int totalBytes = 0;
        for (char ch : cbuf) {
            String str = String.valueOf(ch);
            byte[] bytes = str.getBytes(charset);
            totalBytes += bytes.length;
        }
        this.splitReadLen += totalBytes;
        return readSize;
    }

    @Override
    public void close() throws IOException {
        this.internalReader.close();
    }
}

/**
 * Split reader with auto checking of boundary mark. When reading each split
 * (except for the first one), the reader automatically seek to next accurate
 * record boundary. In addition, for each split, the reader will read through
 * the (estimated) split end till the immediately-next boundary mark. By doing
 * so, the estimated split points can be refined locally to make sure each
 * (updated) split contains whole records (lines). Note: this assumes that each
 * split includes at least one boundary mark (e.g., line terminating character).
 */
class SplitReaderWithBoundaryCheck extends SplitReader {

    private char boundary;

    public SplitReaderWithBoundaryCheck(SourceInputStream stream, Charset charset,
            char boundaryMark) throws IOException {
        System.out.println("----SplitReaderWithBoundaryCheck----");
        this.charset = charset;
        this.boundary = boundaryMark;
        this.splitSize = stream.getSplitSize();
        this.splitReadLen = 0;
        long splitStart = stream.getSplitStart();
        System.out.println("--- splitStart: " + splitStart);
        System.out.println("--- splitSize: " + splitSize);
        if (splitStart != 0) {
            long startSkip = countToFirstMarker(stream);
            stream.skip(startSkip + splitStart);
            this.splitSize -= startSkip;
            System.out.println("--- splitSize by -= " + startSkip + ": " + splitSize);
            System.out.println("--- Split Start: [" + startSkip + "] bytes skipped, "
                    + "making current split to begin from position " + stream.getCurrentPos());
        }
        this.internalReader = new InputStreamReader(stream, charset);
        long endAppend = countToLastMarker(stream);
        this.splitSize += endAppend;
        System.out.println("--- splitSize by += " + endAppend + ": " + splitSize);
        System.out.println("--- Split End: [" + endAppend + "] bytes appended to split end, "
                + "making current total split size " + this.splitSize);
        long currentPos = stream.getCurrentPos();
        System.out.println("--- Processing bytes [" + currentPos + "," + (currentPos + splitSize - 1)
                + "] for file " + stream.getFileName());
        System.out.println("------------------------------------");
    }

    // count the additional bytes that need to be skipped to get to the first
    // marker in the split,
    // this shall not be called for the first split (splitStart = 0)
    private long countToFirstMarker(SourceInputStream originalStream) throws IOException {
        long splitStart = originalStream.getSplitStart();
        SourceInputStream clone = originalStream.cloneStream();
        if (!(clone.getCurrentPos() == 0 && splitStart > 0)) {
            throw new IllegalStateException("SplitReaderWithBoundaryCheck#countToFirstMarker check error.");
        }
        clone.skip(splitStart);
        long additionalBytes = getAdditionalBytes(clone);
        clone.close();
        return additionalBytes;
    }

    // count to the last marker, that is, the first marker beyond the end of
    // current split
    private long countToLastMarker(SourceInputStream originalStream) throws IOException {
        SourceInputStream clone = originalStream.cloneStream();
        // input stream must be a fresh stream just opened
        if (clone.getCurrentPos() != 0) {
            throw new IllegalStateException("SplitReaderWithBoundaryCheck#countToLastMarker check error.");
        }
        // skip to end of split and continue read to the boundary (of a complete
        // line).
        clone.skip(originalStream.getSplitStart() + originalStream.getSplitSize());
        long additionalBytes = getAdditionalBytes(clone);
        clone.close();
        return additionalBytes;
    }

    private long getAdditionalBytes(SourceInputStream stream) throws IOException {
        InputStreamReader reader = new InputStreamReader(stream, this.charset);
        long additionalBytes = 0;
        while (true) {
            int read = reader.read();
            if (read >= 0 && (char) read != this.boundary) {
                additionalBytes++;
                // 如果是中文，则按2个字节计算
                if (isChinese((char) read)) {
                    additionalBytes++;
                }
            } else {
                if ((char) read == this.boundary) {
                    additionalBytes++;
                }
                break;
            }
        }
        return additionalBytes;
    }
    
    // 根据Unicode编码完美的判断中文汉字和符号
    private static boolean isChinese(char c) {
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS 
                || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A 
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B
                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION 
                || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS
                || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION) {
            return true;
        }
        return false;
    }
}
