/*
Navicat MySQL Data Transfer

Source Server         : localhost_3306
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : sdocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2016-06-22 11:10:34
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for sys_sms_station_user
-- ----------------------------
DROP TABLE IF EXISTS `sys_sms_station_user`;
CREATE TABLE `sys_sms_station_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stationid` int(11) DEFAULT NULL,
  `userid` int(11) DEFAULT NULL,
  `type` int(11) DEFAULT '1' COMMENT '短信权限类型 sys_public 0010',
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_stationid_userid_type` (`stationid`,`userid`,`type`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=80 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- ----------------------------
-- Records of sys_sms_station_user
-- ----------------------------
INSERT INTO `sys_sms_station_user` VALUES ('65', '0', '0', '0');
INSERT INTO `sys_sms_station_user` VALUES ('75', '10001', '0', '1');
INSERT INTO `sys_sms_station_user` VALUES ('71', '10001', '0', '2');
INSERT INTO `sys_sms_station_user` VALUES ('76', '10001', '1', '1');
INSERT INTO `sys_sms_station_user` VALUES ('72', '10001', '1', '2');
INSERT INTO `sys_sms_station_user` VALUES ('77', '10001', '2', '1');
INSERT INTO `sys_sms_station_user` VALUES ('73', '10001', '2', '2');
INSERT INTO `sys_sms_station_user` VALUES ('78', '10001', '3', '1');
