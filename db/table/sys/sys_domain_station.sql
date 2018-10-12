/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : ytocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2017-05-22 17:10:50
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for sys_domain_station
-- ----------------------------
DROP TABLE IF EXISTS `sys_domain_station`;
CREATE TABLE `sys_domain_station` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `domainid` int(11) NOT NULL,
  `stationid` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_domain_station` (`domainid`,`stationid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=211 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
