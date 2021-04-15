package org.wltea.analyzer.dic;

/**
 * @author SuZuQi
 * @title: MysqlDict
 * @projectName elasticsearch-analysis-ik
 * @description: 用于加载mysql中的词典和停用词典的线程
 * @date 2021/4/15
 */
public class MysqlDict implements Runnable {

    @Override
    public void run() {
        Dictionary.getSingleton().reLoadMainDict();
    }

}
