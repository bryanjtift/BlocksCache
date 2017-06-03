package me.HeyAwesomePeople.BlocksCache.database;

import me.HeyAwesomePeople.BlocksCache.BlocksCache;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.MessageListener;

import java.util.*;

public class Redis {

    private BlocksCache server;

    private final static String MAP_BLOCKS = "blocks_blocks";
    private final static String MAP_CUBES = "blocks_cubes";
    private final static String CHANNEL_UPDATES = "blocks_updates";
    private final static String UPDATE_FORMAT = "UPDATE uuid WITH BLOCKS=b CUBES=c";

    private RMap<String, Integer> blocks;
    private RMap<String, Integer> cubes;

    private RTopic<String> updatesCh;

    public Redis(BlocksCache server) {
        this.server = server;
        RedissonClient client = Redisson.create();

        blocks = client.getMap(MAP_BLOCKS);
        cubes = client.getMap(MAP_CUBES);

        updatesCh = client.getTopic(CHANNEL_UPDATES);
        registerTopicListener();
    }

    private void registerTopicListener() {
        updatesCh.addListener((channel, msg) -> {
            if (!channel.equalsIgnoreCase(CHANNEL_UPDATES)) return;
            UUID id;
            Integer blocks, cubes;
            String[] exMsg = msg.split(" ");
            if (exMsg[0].equalsIgnoreCase("UPDATE")) {
                try {
                    id = UUID.fromString(exMsg[1]);
                    blocks = Integer.parseInt(exMsg[3].split("=")[1]);
                    cubes = Integer.parseInt(exMsg[4].split("=")[1]);
                    registerPlayer(id, blocks, cubes);
                    server.getProxy().getLogger().info("Updated player currencies. String: '" + msg + "'.");
                } catch (IllegalArgumentException e) {
                    server.getProxy().getLogger().severe("Failed to update through messaging channel! String: '" + msg + "'.");
                }
            }
        });
    }

    public void registerPlayer(UUID id, Integer blocks, Integer cubes) {
        setBlocks(id, blocks);
        setCubes(id, cubes);
    }

    public Integer getBlocks(UUID id) {
        return blocks.get(id.toString());
    }

    private void setBlocks(UUID id, Integer blocks) {
        this.blocks.fastPut(id.toString(), blocks);
    }

    public Integer getCubes(UUID id){
        return cubes.get(id.toString());
    }

    public RMap<String, Integer> getBlocksMap(){
        return blocks;
    }

    public RMap<String, Integer> getCubesMap(){
        return cubes;
    }

    public List<String> getAllKeys() {
        List<String> keys = new ArrayList<>();
        keys.addAll(blocks.keySet());
        for (String key : cubes.keySet()) {
            if (keys.contains(key)) continue;
            keys.add(key);
        }
        return keys;
    }

    private void setCubes(UUID id, Integer cubes) {
        this.cubes.fastPut(id.toString(), cubes);
    }

    public void clearPlayer(UUID id) {
        this.blocks.remove(id.toString());
        this.cubes.remove(id.toString());
    }

}
