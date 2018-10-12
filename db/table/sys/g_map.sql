/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : oceantech

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2017-12-15 08:28:35
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for g_map
-- ----------------------------
DROP TABLE IF EXISTS `g_map`;
CREATE TABLE `g_map` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `url` varchar(255) DEFAULT NULL,
  `format` varchar(255) DEFAULT NULL,
  `version` varchar(255) DEFAULT NULL,
  `tiled` varchar(255) NOT NULL DEFAULT 'true',
  `styles` varchar(255) DEFAULT NULL,
  `transparent` varchar(255) DEFAULT NULL,
  `layers` varchar(255) DEFAULT NULL,
  `code` varchar(255) DEFAULT NULL COMMENT '地图编码',
  `initzoom` int(10) DEFAULT NULL,
  `maxzoom` int(10) DEFAULT NULL,
  `minzoom` int(10) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of g_map
-- ----------------------------
INSERT INTO `g_map` VALUES ('1', 'http://www.google.cn/maps/vt/pb=!1m4!1m3!1i{z}!2i{x}!3i{y}!2m3!1e0!2sm!3i380072576!3m8!2szh-CN!3scn!5e1105!12m4!1e68!2m2!1sset!2sRoadmap!4e0!5m1!1e0', 'image/png', '1.1.1', 'true', null, 'true', '', 'EPSG:900913', '11', '18', '10');
