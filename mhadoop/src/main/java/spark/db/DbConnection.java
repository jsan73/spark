package spark.db;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import scala.runtime.AbstractFunction0;


public class DbConnection extends AbstractFunction0<Connection>  implements Serializable{

	private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(DbConnection.class);
	
	private String driverClassName;
    private String connectionUrl;
    private String userName;
    private String password;

    public DbConnection(String driverClassName, String connectionUrl, String userName, String password) {
        this.driverClassName = driverClassName;
        this.connectionUrl = connectionUrl;
        this.userName = userName;
        this.password = password;
    }
    
	public Connection apply() {
		try {
            Class.forName(driverClassName);
        } catch (ClassNotFoundException e) {
            LOGGER.error("Failed to load driver class", e);
        }

        Properties properties = new Properties();
        properties.setProperty("user", userName);
        properties.setProperty("password", password);

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connectionUrl, properties);
        } catch (SQLException e) {
            LOGGER.error("Connection failed", e);
        }

        return connection;
	}

}
