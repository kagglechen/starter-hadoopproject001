package com.chen.hdfs;

/*
代码步骤：
1. 创建并获取一个客户端对象
2. 执行相关操作命令
3.·关闭资源

上述步骤适用于HDFS和zookeeper
 */

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileSystem {
    private org.apache.hadoop.fs.FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //连接集群nn地址及端口（hadoop102已经实现了ip地址映射）
        URI uri = new URI("hdfs://hadoop102:8020");
        //创建配置文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication","2");
        //传入用户chen
        String user ="chen";
        fs = org.apache.hadoop.fs.FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        //关闭资源
        fs.close();
    }

    //创建目录
    @Test
    public void testmkdir() throws URISyntaxException, IOException, InterruptedException {
//        init();
        fs.mkdirs(new Path("/xiyou/huaguoshan"));
//        close();
    }

    //上传文件
    /*
    参数优先级：
    hdfs_default.xml < hdfs_site.xml < 项目中resource目录下的配置文件 < Configuration对象参数
    (代码中配置对象的优先级最高)
     */
    @Test
    public void testput() throws URISyntaxException, IOException, InterruptedException {
        //参数一:是否删除原数据；参数二:是否允许覆盖；参数三:源文件路径；参数四:目标文件路径
        fs.copyFromLocalFile(false,true,new Path("D:\\hadoop\\databases\\shangguigu\\sunwukong.txt"), new Path("/xiyou/huaguoshan"));
    }

    //下载文件
    @Test
    public void testget() throws URISyntaxException, IOException, InterruptedException {
        //参数一：是否删除原（hdfs中）文件；参数二：源文件路径；参数三：目标文件路径;参数四：crc数据校验（true表示关闭）
        fs.copyToLocalFile(false,new Path("/xiyou/huaguoshan/sunwukong.txt"),new Path("D:\\hadoop\\databases\\DownloadFromHDFS\\"),false);
        //下载后生成的crc文件时文件校验文件（校验码），在hdfs传输时通过加密算法生成crc文件及校验码，下载数据后同样生成一个校验码，如果两个检验码一样则表示数据完整
    }

    //删除
    @Test
    public void testrm() throws URISyntaxException, IOException, InterruptedException {
        //删除文件
        //参数二：要删除的文件路径;参数一：是否递归删除
        fs.delete(new Path("/xiyou/huaguoshan/sunwukong.txt"),false);

        //删除空目录
        fs.delete(new Path("/xiyou"),false);

        //删除非空目录(需要递归删除)
        fs.delete(new Path("/xiyou/huaguoshan"),true);
    }

    //文件更名和移动(也可对目录更名)
    @Test
    public void testmv() throws URISyntaxException, IOException, InterruptedException {
        //参数一：源文件路径；参数二：目标文件路径(作用更换名字或更换位置)
        fs.rename(new Path("/xiyou/huaguoshan/sunwukong.txt"),new Path("/xiyou/huaguoshan/swk.txt"));

        //更换文件位置并改名
        //参数：源文件路径；目标文件路径
        fs.rename(new Path("/xiyou/huaguoshan/swk.txt"),new Path("/xiyou/ss.txt"));
    }

    //获取文件信息
    @Test
    public void fileDetail() throws URISyntaxException, IOException, InterruptedException {
        //获取文件信息(RemoteIterator是迭代器)
        //参数：路径；参数：是否递归
        RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            org.apache.hadoop.fs.LocatedFileStatus next = listFiles.next();

            System.out.println("=========="+next.getPath()+"==========");
            //获取文件名称
            String name = next.getPath().getName();
            //获取文件大小
            long len = next.getLen();
            //获取权限
            String permission = next.getPermission().toString();
            //获取所有者
            String owner = next.getOwner();
            //获取组
            String group = next.getGroup();
            //获取块大小
            long blockSize = next.getBlockSize();
            //获取修改时间
            long modificationTime = next.getModificationTime();
            //获取文件备份数量
            short replication = next.getReplication();

            System.out.println("文件权限："+permission);
            System.out.println("文件所属用户："+owner);
            System.out.println("文件所属组："+group);
            System.out.println("文件大小："+len);
            System.out.println("文件修改时间："+modificationTime);
            System.out.println("文件备份数量："+replication);
            System.out.println("文件块大小："+blockSize);
            System.out.println("文件名称："+name);

            //获取文件块信息
            BlockLocation[] blockLocations = next.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    @Test
    public void testFile() throws URISyntaxException, IOException, InterruptedException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isDirectory()){
                System.out.println("目录："+fileStatus.getPath().getName());
            }else{
                System.out.println("文件："+fileStatus.getPath().getName());
            }
        }
    }
}
