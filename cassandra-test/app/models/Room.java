package models;

import com.netflix.astyanax.mapping.Column;
import com.netflix.astyanax.mapping.Id;
import play.modules.cassandra.Model;
import play.modules.cassandra.annotations.Entity;

import javax.persistence.ManyToMany;
import java.util.List;

@Entity(columnFamily = "ROOM")
public class Room extends Model {
    @Id("GUID")
    public String guid;

    @Column("name")
    public String name;

    @ManyToMany
    public List<Game> games;
}
