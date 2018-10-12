/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : ytsocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2018-05-09 15:20:50
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for sys_domain_threshold
-- ----------------------------
DROP TABLE IF EXISTS `sys_domain_threshold`;
CREATE TABLE `sys_domain_threshold` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `levelid` int(11) NOT NULL,
  `mincal` int(11) NOT NULL DEFAULT '1' COMMENT '1 <= 2 <',
  `min` double NOT NULL DEFAULT '0',
  `maxcal` int(11) NOT NULL DEFAULT '1' COMMENT '1>=  2>',
  `max` double NOT NULL DEFAULT '0',
  `indicatorcode` varchar(50) DEFAULT NULL COMMENT '参数',
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_level_indicator` (`levelid`,`indicatorcode`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8;
