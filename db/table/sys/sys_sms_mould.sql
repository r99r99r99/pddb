/*
Navicat MySQL Data Transfer

Source Server         : localhost_3306
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : sdocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2016-06-22 11:11:37
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for sys_sms_mould
-- ----------------------------
DROP TABLE IF EXISTS `sys_sms_mould`;
CREATE TABLE `sys_sms_mould` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stationid` int(11) DEFAULT NULL,
  `type` int(11) DEFAULT '1' COMMENT '1:预警告警模板',
  `mould` text COLLATE utf8_bin,
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_stationid_type` (`stationid`,`type`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- ----------------------------
-- Records of sys_sms_mould
-- ----------------------------
INSERT INTO `sys_sms_mould` VALUES ('1', '0', '1', 0x31313333);
INSERT INTO `sys_sms_mould` VALUES ('2', '10001', '1', 0xE5BD93E5898DE7AB99E782B97B77704E616D657DE4BA8E7B636F6C6C6563745F74696D657DE79B91E6B58BE79A845BE58F82E695B07B696E64696361746F724E616D657DE695B0E580BCE4B8BA7B646174617D7B756E69744E616D657D2C7B6966754F647D7B6C696D6974446174617D7B756E69744E616D657D2C5D);
INSERT INTO `sys_sms_mould` VALUES ('3', '10001', '2', 0xE5BD93E5898DE7AB99E782B97B73746174696F6E4E616D657DE59CA87B636F6C6C65637454696D657DE697B6E79B91E6B58BE79A845BE8AEBEE5A4877B6E616D657D2C5DE69CAAE68C89E8AEA1E58892E79B91E6B58BE7BB93E69E9C2C);
INSERT INTO `sys_sms_mould` VALUES ('4', '0', '2', 0x31313333);
