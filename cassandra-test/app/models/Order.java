package models;

import com.netflix.astyanax.mapping.Column;
import com.netflix.astyanax.mapping.Id;
import play.Logger;
import play.modules.cassandra.Model;
import play.modules.cassandra.annotations.Counter;
import play.modules.cassandra.annotations.Entity;

import java.math.BigDecimal;
import java.util.Date;

@Entity(columnFamily="ORDER")
public class Order extends Model {
    @Id("ORDERID")
    public String id;

    @Column("SKU")
    public Product product;

    @Column("USERID")
    public Account user;

    @Counter
    @Column("QUANTITY")
    public long quantity;

    @Column("TRANSACTIONID")
    public String transactionId;

    @Column("DATEINITIATED")
    public Date dateInitiated;

    @Column("DATECOMPLETED")
    public Date dateCompleted;

    @Column("PRICEPAID")
    public BigDecimal pricePaid;

    @Counter(scale="2")
    @Column("REFUND")
    public BigDecimal refund;

    @Column("APPVERSION")
    public String appVersion;

    @Column("RECEIPTSTATUS")
    public String receiptStatus;

    public Order() {
        Logger.info("Order created");
    }

    public String toString() {
        return String.format("Order[quantity:%d,refund:%s]", quantity, (null == refund ? "0.00" : refund.toString()));
    }

    public static void present() {
        Logger.info("Order is present");
    }
}
