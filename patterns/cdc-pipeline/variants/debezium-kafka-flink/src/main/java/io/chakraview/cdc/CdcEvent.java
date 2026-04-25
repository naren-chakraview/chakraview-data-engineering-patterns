package io.chakraview.cdc;

public class CdcEvent {
    public String orderId;
    public String customerId;
    public String status;
    public long   amountCents;
    public String placedAt;
    public boolean deleted;

    public boolean isDeleted() { return deleted; }

    @Override
    public String toString() {
        return String.format("CdcEvent{orderId=%s, status=%s, deleted=%s}",
            orderId, status, deleted);
    }
}
