/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : ytocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2017-05-22 17:10:37
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_domain
-- ----------------------------
DROP TABLE IF EXISTS `aiot_domain`;
CREATE TABLE `aiot_domain` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(50) COLLATE utf8_bin NOT NULL COMMENT '功能区编码',
  `name` varchar(50) COLLATE utf8_bin NOT NULL COMMENT '功能区名称',
  `remark` varchar(255) COLLATE utf8_bin DEFAULT NULL,
  `isactive` int(11) NOT NULL DEFAULT '1' COMMENT '0004',
  `ordercode` varchar(3) COLLATE utf8_bin NOT NULL DEFAULT '999',
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_domain_code` (`code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=21 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
