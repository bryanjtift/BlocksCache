package me.HeyAwesomePeople.BlocksCache.database;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import me.HeyAwesomePeople.BlocksCache.BlocksCache;
import me.HeyAwesomePeople.BlocksCache.Messages;

import java.io.PrintWriter;
import java.sql.*;
import java.util.Properties;
import java.util.UUID;

public class MySQL {

    private HikariDataSource hikari;

    public MySQL(BlocksCache fp) {
        Properties props = new Properties();
        props.setProperty("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        props.setProperty("dataSource.serverName", fp.config.getString("mysql.host"));
        props.setProperty("dataSource.port", fp.config.getString("mysql.port"));
        props.setProperty("dataSource.databaseName", fp.config.getString("mysql.databaseName"));
        props.setProperty("dataSource.user", fp.config.getString("mysql.user"));
        props.setProperty("dataSource.password", fp.config.getString("mysql.password"));
        props.setProperty("dataSource.setMaximumPoolSize", "10");
        props.setProperty("dataSource.useServerPrepStmts", "true");
        props.setProperty("dataSource.cachePrepStmts", "true");
        props.setProperty("dataSource.prepStmtCacheSize", "250");
        props.setProperty("dataSource.prepStmtCacheSqlLimit", "2048");
        props.put("dataSource.logWriter", new PrintWriter(System.out));

        HikariConfig config = new HikariConfig(props);
        hikari = new HikariDataSource(config);
        hikari.setLeakDetectionThreshold(10000);

        try {
            createTables();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        fp.getProxy().getLogger().info(Messages.CONNECT_SUCCESS_MYSQL);
    }

    private HikariDataSource getHikari() {
        return this.hikari;
    }

    private void createTables() throws SQLException {
        Connection connection = getHikari().getConnection();
        Statement statement = connection.createStatement();
        statement.executeUpdate("CREATE TABLE IF NOT EXISTS blocks_playerdata" +
                " (id VARCHAR(36) UNIQUE, blocks BIGINT(20), cubes BIGINT(20))");
        connection.close();
    }

    public Integer[] retrieveData(UUID id, BlocksCache plugin) throws SQLException {
        Integer blocks, cubes, firstTime;
        Connection connection = getHikari().getConnection();
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM blocks_playerdata WHERE id=? LIMIT 1");
        statement.setString(1, id.toString());
        ResultSet result = statement.executeQuery();

        if (result.next()) {
            blocks = result.getInt(2);
            cubes = result.getInt(3);
            firstTime = 0;
        } else {
            blocks = plugin.config.getInt("starting_balance.blocks");
            cubes = plugin.config.getInt("starting_balance.cubes");
            firstTime = 1;
        }

        connection.close();
        return new Integer[] {blocks, cubes, firstTime};
    }

    public void uploadData(UUID id, Integer blocks, Integer cubes) throws SQLException {
        Connection connection =  getHikari().getConnection();
        PreparedStatement statement = connection.prepareStatement("INSERT INTO blocks_playerdata (id, blocks, cubes) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id=?, blocks=?, cubes=?");

        statement.setString(1, id.toString());
        statement.setInt(2, blocks);
        statement.setInt(3, cubes);
        statement.setString(4, id.toString());
        statement.setInt(5, blocks);
        statement.setInt(6, cubes);
        statement.execute();

        connection.close();
    }

    public void uploadAllData(Redis redis) throws SQLException {
        Connection connection =  getHikari().getConnection();
        PreparedStatement statement = connection.prepareStatement("INSERT INTO blocks_playerdata (id, blocks, cubes) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE id=?, blocks=?, cubes=?");

        for (String id : redis.getAllKeys()) {
            statement.setString(1, id);
            statement.setInt(2, redis.getBlocks(UUID.fromString(id)));
            statement.setInt(3, redis.getCubes(UUID.fromString(id)));
            statement.setString(4, id);
            statement.setInt(5, redis.getBlocks(UUID.fromString(id)));
            statement.setInt(6, redis.getCubes(UUID.fromString(id)));

            statement.addBatch();
        }

        statement.executeBatch();
        connection.close();
    }

}
