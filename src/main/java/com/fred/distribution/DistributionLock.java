package com.fred.distribution;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Hello world!
 */
public class DistributionLock implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(DistributionLock.class);

    //确保连接zk成功；
    private CountDownLatch connectLatch = new CountDownLatch(1);
    private static final CountDownLatch threadSemaphore = new CountDownLatch(10);
    private ZooKeeper zooKeeper;
    private String LOG_PREFIX_OF_THREAD;

    private String selfPath;
    private String waitPath;

    private static final String DISK_LOCK_PATH = "/dis_lock";
    private static final String SUB_PATH = DISK_LOCK_PATH + "/sub";


    public DistributionLock(int threadNum) {
        this.LOG_PREFIX_OF_THREAD = "第" + threadNum + "个线程: ";
    }

    private void createConnection() {

        try {
            zooKeeper = new ZooKeeper("localhost:2181", 50000, this);
            connectLatch.await();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void getContent() {

        try {
            List<String> nodes = zooKeeper.getChildren("/", this);

            for (String node : nodes) {
                LOG.error("node: " + node);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void getLock() throws KeeperException, InterruptedException {

        selfPath = zooKeeper.create(SUB_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        LOG.error(LOG_PREFIX_OF_THREAD + "创建锁路径:" + selfPath);

        if (checkMinPath()) {
            getLockSuccess();
        }
    }

    private void getLockSuccess() throws KeeperException, InterruptedException {

        if (zooKeeper.exists(this.selfPath, false) == null) {
            LOG.error(LOG_PREFIX_OF_THREAD + "本节点已不在了...");
            return;
        }
        LOG.error(LOG_PREFIX_OF_THREAD + "获取锁成功，赶紧干活！");
        Thread.sleep(2000);
        LOG.error(LOG_PREFIX_OF_THREAD + "删除本节点：" + selfPath);
        zooKeeper.delete(this.selfPath, -1);
        releaseConnection();
        threadSemaphore.countDown();
    }

    private void releaseConnection() {
        if (this.zooKeeper != null) {
            try {
                this.zooKeeper.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        LOG.error(LOG_PREFIX_OF_THREAD + "释放连接");
    }

    private boolean createPath(String path, String data, boolean needWatch) throws KeeperException, InterruptedException {
        if (zooKeeper.exists(path, needWatch) == null) {
            LOG.error(LOG_PREFIX_OF_THREAD + "节点创建成功, Path: "
                    + zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
                    + ", content: " + data);
        }
        return true;
    }

    public boolean checkMinPath() throws KeeperException, InterruptedException {
        List<String> subNodes = zooKeeper.getChildren(DISK_LOCK_PATH, false);
        Collections.sort(subNodes);
        int index = subNodes.indexOf(selfPath.substring(DISK_LOCK_PATH.length() + 1));
        switch (index) {
            case -1: {
                LOG.error(LOG_PREFIX_OF_THREAD + "本节点已不在了..." + selfPath);
                return false;
            }
            case 0: {
                LOG.error(LOG_PREFIX_OF_THREAD + "子节点中，我果然是老大" + selfPath);
                return true;
            }
            default: {
                this.waitPath = DISK_LOCK_PATH + "/" + subNodes.get(index - 1);
                LOG.error(LOG_PREFIX_OF_THREAD + "获取子节点中，排在我前面的" + waitPath);
                try {
                    zooKeeper.getData(waitPath, true, new Stat());
                    return false;
                } catch (KeeperException e) {
                    if (zooKeeper.exists(waitPath, false) == null) {
                        LOG.error(LOG_PREFIX_OF_THREAD + "子节点中，排在我前面的" + waitPath + "已失踪，幸福来得太突然?");
                        return checkMinPath();
                    } else {
                        throw e;
                    }
                }
            }

        }

    }


    @Override
    public void process(WatchedEvent event) {

        if (event == null) {
            return;
        }
        Event.KeeperState keeperState = event.getState();
        Event.EventType eventType = event.getType();
        if (Event.KeeperState.SyncConnected == keeperState) {
            if (Event.EventType.None == eventType) {
                LOG.error(LOG_PREFIX_OF_THREAD + "成功连接上ZK服务器");
                connectLatch.countDown();
            } else if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(waitPath)) {
                LOG.error(LOG_PREFIX_OF_THREAD + "收到情报，排我前面的家伙已挂，我是不是可以出山了？");
                try {
                    if (checkMinPath()) {
                        getLockSuccess();
                    }
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } else if (Event.KeeperState.Disconnected == keeperState) {
            LOG.error(LOG_PREFIX_OF_THREAD + "与ZK服务器断开连接");
        } else if (Event.KeeperState.AuthFailed == keeperState) {
            LOG.error(LOG_PREFIX_OF_THREAD + "权限检查失败");
        } else if (Event.KeeperState.Expired == keeperState) {
            LOG.error(LOG_PREFIX_OF_THREAD + "会话失效");
        }
    }

    public static void main(String[] args) {

        for (int i = 0; i < 10; i++) {
            final int threadId = i + 1;

            new Thread() {

                @Override
                public void run() {
                    try {
                        DistributionLock dc = new DistributionLock(threadId);
                        dc.createConnection();
                        //GROUP_PATH不存在的话，由一个线程创建即可；
                        synchronized (threadSemaphore) {
                            dc.createPath(DISK_LOCK_PATH, "该节点由线程" + threadId + "创建", true);
                        }
                        dc.getLock();
                    } catch (Exception e) {
                        LOG.error("【第" + threadId + "个线程】 抛出的异常：");
                        e.printStackTrace();
                    }
                }
            }.start();
        }
        try {
            threadSemaphore.await();
            LOG.error("所有线程运行结束!");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}