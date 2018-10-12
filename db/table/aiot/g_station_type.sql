/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : ytocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2017-05-02 14:45:37
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for g_station_type
-- ----------------------------
DROP TABLE IF EXISTS `g_station_type`;
CREATE TABLE `g_station_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(10) COLLATE utf8_bin DEFAULT NULL COMMENT '站点类型编码',
  `name` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `remark` varchar(2000) COLLATE utf8_bin DEFAULT NULL,
  `icon` varchar(255) COLLATE utf8_bin DEFAULT NULL COMMENT '该类站点的图标,用于在小地图上显示',
  `isactive` int(11) DEFAULT '1',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
