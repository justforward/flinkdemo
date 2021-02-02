package com.bigdata.operator.speech;

import com.didi.wujie.recommend.flink.GroupSpeechTotal;
import com.didi.wujie.recommend.flink.cache.CacheConf;
import com.didi.wujie.recommend.flink.cache.Client;
import com.didi.wujie.recommend.flink.cache.FusionClientPool;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wangtan
 * @Date 2021/1/14
 */
public class SaveResultToCache implements MapFunction<GroupSpeechTotal.Result, String> {
    private static final Logger logger = LoggerFactory.getLogger(SaveResultToCache.class);
    @Override
    public String map(GroupSpeechTotal.Result result) throws Exception {
        logger.info("final result: "+result);
        String key = result.getGroupId() + "_" + "speech" + "_" + "total";
        Client client = FusionClientPool.borrowObject();
        client.set(key, result.getTotal().toString(), CacheConf.getTtl());
        FusionClientPool.returnObject(client);
        return key;
    }
}
