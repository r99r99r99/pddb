/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.80
Source Server Version : 50713
Source Host           : 192.168.10.80:3306
Source Database       : ytsocean

Target Server Type    : MYSQL
Target Server Version : 50713
File Encoding         : 65001

Date: 2018-06-27 15:49:07
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_beach_threshold
-- ----------------------------
DROP TABLE IF EXISTS `aiot_beach_threshold`;
CREATE TABLE `aiot_beach_threshold` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(255) NOT NULL,
  `levelid` int(11) NOT NULL,
  `mincal` int(11) DEFAULT NULL COMMENT '下限的计算方式 1 >= 0 >',
  `min` double DEFAULT NULL,
  `maxcal` int(11) DEFAULT NULL COMMENT '上限的计算方式  1<= 0<',
  `max` double DEFAULT NULL,
  `data` double DEFAULT NULL COMMENT '当TYPE=2时,计算用此参数 当type=1时,用上方的范围计算方式',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=71 DEFAULT CHARSET=utf8;
