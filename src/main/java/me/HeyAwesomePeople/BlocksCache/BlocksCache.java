package me.HeyAwesomePeople.BlocksCache;

import me.HeyAwesomePeople.BlocksCache.database.MySQL;
import me.HeyAwesomePeople.BlocksCache.database.Redis;
import net.md_5.bungee.api.ProxyServer;
import net.md_5.bungee.api.chat.TextComponent;
import net.md_5.bungee.api.event.LoginEvent;
import net.md_5.bungee.api.event.PlayerDisconnectEvent;
import net.md_5.bungee.api.plugin.Listener;
import net.md_5.bungee.api.plugin.Plugin;
import net.md_5.bungee.config.Configuration;
import net.md_5.bungee.config.ConfigurationProvider;
import net.md_5.bungee.config.YamlConfiguration;
import net.md_5.bungee.event.EventHandler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.UUID;

public class BlocksCache extends Plugin implements Listener {

    private MySQL mySQL;
    private Redis redis;

    public Configuration config;

    @Override
    public void onEnable() {
        readyConfig();

        mySQL = new MySQL(this);
        redis = new Redis(this);

        getProxy().getPluginManager().registerListener(this, this);

    }

    @Override
    public void onDisable() {
        try {
            mySQL.uploadAllData(redis);
            for (String id : redis.getAllKeys()) {
                redis.clearPlayer(UUID.fromString(id));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void readyConfig() {
        if (!getDataFolder().exists())
            //noinspection ResultOfMethodCallIgnored
            getDataFolder().mkdir();

        File file = new File(getDataFolder(), "config.yml");

        if (!file.exists()) {
            try (InputStream in = getResourceAsStream("config.yml")) {
                Files.copy(in, file.toPath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            config = ConfigurationProvider.getProvider(YamlConfiguration.class).load(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @EventHandler
    public void onPostLogin(LoginEvent e) {
        e.registerIntent(this);
        ProxyServer.getInstance().getScheduler().runAsync(this, () -> {
            Integer[] data;

            try {
                data = mySQL.retrieveData(e.getConnection().getUniqueId(), BlocksCache.this);
            } catch (SQLException e1) {
                e.setCancelReason(new TextComponent("Failed to load MySQL currency data. Report error to admins."));
                e.setCancelled(true);
                e1.printStackTrace();
                return;
            }

            redis.registerPlayer(e.getConnection().getUniqueId(), data[0], data[1]);
            if (data[2] == 1)
                uploadData(e.getConnection().getUniqueId());
            e.completeIntent(BlocksCache.this);
        });
    }

    @EventHandler
    public void onLogout(PlayerDisconnectEvent e) {
        ProxyServer.getInstance().getScheduler().runAsync(this, () -> uploadData(e.getPlayer().getUniqueId()));
    }

    private void uploadData(UUID id) {
        try {
            Integer[] data = new Integer[]{redis.getBlocks(id), redis.getCubes(id)};
            mySQL.uploadData(id, data[0], data[1]);
            redis.clearPlayer(id);
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
    }

}
