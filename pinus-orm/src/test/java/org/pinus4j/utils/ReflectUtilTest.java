package org.pinus4j.utils;

import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.pinus4j.BaseTest;
import org.pinus4j.entity.TestEntity;
import org.pinus4j.utils.BeansUtil;

public class ReflectUtilTest extends BaseTest {
    
    @Test
    public void testGetPropery() throws Exception {
        TestEntity entity = createEntity();
        Assert.assertEquals('a', BeansUtil.getProperty(entity, "testChar"));
    }

    @Test
    public void testSetProperty() throws Exception {
        TestEntity entity = new TestEntity();
        BeansUtil.setProperty(entity, "testString", "test name");
        Assert.assertEquals("test name", entity.getTestString());

        BeansUtil.setProperty(entity, "oTestInt", 1);
        Assert.assertEquals(1, entity.getOTestInt().intValue());
    }

    @Test
    public void testCloneWithGivenFieldObjectString() throws Exception {
        TestEntity entity = createEntity();
        TestEntity clone = (TestEntity) BeansUtil.cloneWithGivenField(entity, "testInt", "testDouble");
        Assert.assertEquals(entity.getTestInt(), clone.getTestInt());
        Assert.assertEquals(entity.getTestDouble(), clone.getTestDouble());
        Assert.assertEquals(0.0f, clone.getTestFloat());
        Assert.assertNotNull(entity.getTestString());
        Assert.assertNull(clone.getTestString());
    }

    @Test
    public void testCopyProperties() throws Exception {
        TestEntity source = createEntity();
        TestEntity target = new TestEntity();
        BeansUtil.copyProperties(source, target);
        Assert.assertEquals(source, target);
    }

}
