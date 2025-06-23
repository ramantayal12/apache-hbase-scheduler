package org.gojo.learn.scheduler.sdk.controller;

import org.gojo.learn.scheduler.sdk.hbase.HbaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("/db-controller")
public class DBController {

    private final HbaseService hbaseService;

    @Autowired
    public DBController(HbaseService hbaseService) {
        this.hbaseService = hbaseService;
    }

    @PostMapping
    public ResponseEntity<?> createColoumnFamily(
            @RequestParam String tableName,
            @RequestParam String columnFamilyName
    ) throws IOException {
        hbaseService.createColumnFamily(tableName, columnFamilyName);
        return ResponseEntity.ok().build();
    }
}
