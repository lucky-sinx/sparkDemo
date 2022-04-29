package sparksql;

import java.io.Serializable;

/**
 * Aggregator<IN,BUF,OUT>
 */
public class AvgBuffer implements Serializable {
    public Long sum;
    public Long cnt;

    public AvgBuffer() {
    }

    public AvgBuffer(Long sum, Long cnt) {
        this.sum = sum;
        this.cnt = cnt;
    }

    public Long getSum() {
        return sum;
    }

    public Long getCnt() {
        return cnt;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }

    public void setCnt(Long cnt) {
        this.cnt = cnt;
    }
}
