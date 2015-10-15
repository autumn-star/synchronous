package com.synchro.util;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by xingxing.duan on 2015/5/13.
 * 获取spring容器，以访问容器中定义的其他bean
 */
public class SpringContextUtils {

    // Spring应用上下文环境
    private static ApplicationContext applicationContext = new ClassPathXmlApplicationContext(new String[]{"spring.xml"});

    /**
     * 获取对象,通过Class类型拿到实例对象，前提实例名字和类名一致且第一个字符为小写
     *
     * @param targetClass
     * @param <T>
     * @return
     * @throws BeansException
     */
    @SuppressWarnings("unchecked")
	public static <T> T getBean(Class<T> targetClass) throws BeansException {
        String clazzName = StringUtils.substringAfterLast(targetClass.getName(), ".");
        String instanceName = clazzName.substring(0, 1).toLowerCase() + clazzName.substring(1);
        return (T) SpringContextUtils.applicationContext.getBean(instanceName);
    }
    
}