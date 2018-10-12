/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : hnocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2017-12-21 14:38:03
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_watch_point
-- ----------------------------
DROP TABLE IF EXISTS `aiot_watch_point`;
CREATE TABLE `aiot_watch_point` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `code` varchar(32) NOT NULL,
  `title` varchar(16) NOT NULL,
  `latitude` double DEFAULT NULL COMMENT '维度',
  `longitude` double DEFAULT NULL COMMENT '经度',
  `station_gateway` varchar(32) DEFAULT NULL COMMENT '站点接口',
  `stationtype_id` bigint(20) DEFAULT NULL COMMENT '站点类型',
  `region_id` bigint(20) DEFAULT NULL COMMENT '地区编号',
  `companyid` varchar(50) DEFAULT '0001',
  `brief` varchar(220) DEFAULT NULL,
  `detail` varchar(1000) DEFAULT NULL,
  `pic` varchar(100) DEFAULT NULL,
  `isactive` int(11) DEFAULT '1' COMMENT '1',
  `watertype` int(11) DEFAULT '1' COMMENT '水质标准类型1:海洋水质,2:地表水质',
  `ordercode` varchar(50) DEFAULT NULL,
  `ifsms` int(255) DEFAULT '0' COMMENT '0:代表关闭 1:代表开启',
  `warndistance` double DEFAULT '0' COMMENT '当浮标飘走后的告警距离',
  `standard` int(11) NOT NULL DEFAULT '2' COMMENT '该站点的标准水质等级标准',
  PRIMARY KEY (`id`),
  UNIQUE KEY `code` (`code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=10006 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of aiot_watch_point
-- ----------------------------
INSERT INTO `aiot_watch_point` VALUES ('10004', 'CMFB06', '澄迈湾', '19.9696', '109.9756', '澄迈湾', '3', '299', '0001', null, null, 'stationImages/liaohe_station_small.jpg', '1', '2', '003', '0', '0', '2');
INSERT INTO `aiot_watch_point` VALUES ('10005', 'HKFB05', '海口湾', '20.0405', '110.2417', '海口湾', '3', '299', '0001', null, null, 'stationImages/sh0301.jpg', '1', '2', '004', '0', '0', '2');
