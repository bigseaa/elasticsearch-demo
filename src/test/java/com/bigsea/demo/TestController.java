package com.bigsea.demo;

import com.bigsea.controller.HelloController;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

public class TestController {

    private MockHttpServletRequest request;

    private MockHttpServletResponse response;

    @Autowired
    private HelloController helloController;

    @Before
    public void setUp() {
        request = new MockHttpServletRequest();
        request.setCharacterEncoding("UTF-8");
        response = new MockHttpServletResponse();
    }

    @Test
    public void test() {
        helloController.test();
    }

}
