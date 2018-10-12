/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : yantaiocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2016-10-17 14:51:55
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_device_collect
-- ----------------------------
DROP TABLE IF EXISTS `aiot_device_collect`;
CREATE TABLE `aiot_device_collect` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stationid` int(11) NOT NULL,
  `deviceid` int(11) NOT NULL,
  `starttime` time DEFAULT NULL,
  `endtime` time DEFAULT NULL,
  `collecttime` time DEFAULT NULL,
  `daynum` int(11) NOT NULL DEFAULT '0' COMMENT '此列用于标记于所计算的日期的时间运算.',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- ----------------------------
-- Records of aiot_device_collect
-- ----------------------------
INSERT INTO `aiot_device_collect` VALUES ('1', '10001', '21', '00:00:00', '03:00:00', '00:05:00', '0');
INSERT INTO `aiot_device_collect` VALUES ('2', '10001', '21', '03:00:00', '06:00:00', '03:05:00', '0');
INSERT INTO `aiot_device_collect` VALUES ('3', '10001', '21', '06:00:00', '09:00:00', '06:05:00', '0');
INSERT INTO `aiot_device_collect` VALUES ('4', '10001', '21', '09:00:00', '12:00:00', '09:05:00', '0');
INSERT INTO `aiot_device_collect` VALUES ('5', '10001', '21', '12:00:00', '15:00:00', '12:05:00', '0');
INSERT INTO `aiot_device_collect` VALUES ('6', '10001', '21', '15:00:00', '18:00:00', '15:05:00', '0');
INSERT INTO `aiot_device_collect` VALUES ('7', '10001', '21', '18:00:00', '21:00:00', '18:05:00', '0');
INSERT INTO `aiot_device_collect` VALUES ('8', '10001', '21', '21:00:00', '24:00:00', '21:05:00', '0');
