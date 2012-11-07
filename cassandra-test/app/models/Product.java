package models;

import com.netflix.astyanax.mapping.Id;
import play.modules.cassandra.Model;
import play.modules.cassandra.annotations.Entity;

@Entity(columnFamily="PRODUCT")
public class Product extends Model {
    @Id("SKU")
    public String sku;
}