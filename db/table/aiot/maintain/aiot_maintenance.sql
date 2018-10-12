/*
Navicat MySQL Data Transfer

Source Server         : localhost_3306
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : sdocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2016-07-05 11:32:16
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_maintenance
-- ----------------------------
DROP TABLE IF EXISTS `aiot_maintenance`;
CREATE TABLE `aiot_maintenance` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `wpid` int(11) DEFAULT NULL,
  `deviceid` int(11) DEFAULT NULL,
  `begin_time` timestamp NULL DEFAULT NULL,
  `end_time` timestamp NULL DEFAULT NULL,
  `state` int(255) DEFAULT '2' COMMENT 'sys_public 0014 默认为正在维护',
  `userid` int(11) DEFAULT NULL,
  `collect_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `type` int(255) DEFAULT '1' COMMENT 'syspublic 0015  ',
  `reason` varchar(2550) COLLATE utf8_bin DEFAULT NULL,
  `result` varchar(2550) COLLATE utf8_bin DEFAULT NULL,
  `material` varchar(2550) COLLATE utf8_bin DEFAULT NULL COMMENT '所需材料',
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_wpid_time` (`wpid`,`begin_time`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- ----------------------------
-- Records of aiot_maintenance
-- ----------------------------
INSERT INTO `aiot_maintenance` VALUES ('1', '10001', '33', '2016-03-07 00:00:00', '2016-07-04 10:35:33', '2', '1', '2016-03-17 08:03:58', '1', 'reason', 'result', null);
INSERT INTO `aiot_maintenance` VALUES ('2', '10001', '33', '2016-02-07 00:00:00', '2016-07-04 10:35:38', '2', '1', '2016-03-17 08:33:52', '1', '2222', '3333', null);
INSERT INTO `aiot_maintenance` VALUES ('4', '10002', '33', '2016-03-17 00:00:00', '2016-07-04 10:35:42', '2', '1', '2016-03-17 10:14:28', '1', '例行维护', '崩溃了,这个生意不做了', null);
INSERT INTO `aiot_maintenance` VALUES ('5', '10001', '32', '2016-07-02 09:08:55', '2016-07-02 09:08:55', '3', '1', '2016-07-05 09:09:14', '1', null, '相当成功', '电线啊乱七八糟的');
INSERT INTO `aiot_maintenance` VALUES ('6', '10001', '30', '2016-06-02 09:08:55', '2016-06-07 09:08:55', '2', '1', '2016-07-05 10:43:39', '1', '3333', '3333', '333');
