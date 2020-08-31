package com.example;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class JavaSparkAppTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JavaSparkAppTest.class);

    @Test
    public void shouldAnswerWithTrue() {
        LOGGER.info("JUnit Testing ....!");
        assertTrue(true);
    }
}
