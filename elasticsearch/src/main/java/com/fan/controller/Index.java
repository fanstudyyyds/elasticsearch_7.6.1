package com.fan.controller;

import com.fan.service.Es;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Controller
public class Index {

    @Autowired
    private Es es;

    @RequestMapping({"/", "index"})
    public String index() {
        return "index";
    }

    @RequestMapping({"/search/{keyword}/{pageNo}/{pageSize}"})
    @ResponseBody
    public List<Map<String, Object>> searchPage(@PathVariable("keyword") String keyword, @PathVariable("pageNo")int pageNo, @PathVariable("pageSize")int pageSize) throws IOException {
        System.out.println(keyword);
        return es.searchPage2(keyword, pageNo, pageSize);
    }
}
