/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : liaoheocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2018-03-28 09:25:09
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_device_alarm_config
-- ----------------------------
DROP TABLE IF EXISTS `aiot_device_alarm_config`;
CREATE TABLE `aiot_device_alarm_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(255) NOT NULL,
  `title` varchar(255) NOT NULL,
  `ordercode` varchar(3) NOT NULL DEFAULT '999',
  `remark` varchar(255) DEFAULT NULL COMMENT '备注',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of aiot_device_alarm_config
-- ----------------------------
INSERT INTO `aiot_device_alarm_config` VALUES ('1', '001', '超量程', '001', null);
INSERT INTO `aiot_device_alarm_config` VALUES ('2', '002', '空气报警', '002', null);
INSERT INTO `aiot_device_alarm_config` VALUES ('3', '003', '漏液报警', '003', null);
INSERT INTO `aiot_device_alarm_config` VALUES ('4', '004', '标准液校准异常', '004', null);
INSERT INTO `aiot_device_alarm_config` VALUES ('5', '005', '空白液校准异常', '005', null);
INSERT INTO `aiot_device_alarm_config` VALUES ('6', '006', '缺液报警', '006', null);
