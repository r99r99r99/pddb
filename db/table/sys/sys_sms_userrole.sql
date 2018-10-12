/*
Navicat MySQL Data Transfer

Source Server         : localhost_3306
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : sdocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2016-06-22 11:12:54
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for sys_sms_userrole
-- ----------------------------
DROP TABLE IF EXISTS `sys_sms_userrole`;
CREATE TABLE `sys_sms_userrole` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `userid` int(11) DEFAULT NULL,
  `ambegin` time DEFAULT NULL,
  `amend` time DEFAULT NULL,
  `pmbegin` time DEFAULT NULL,
  `pmend` time DEFAULT NULL,
  `mon` int(11) DEFAULT '1',
  `tues` int(11) DEFAULT '1',
  `wed` int(11) DEFAULT '1',
  `thur` int(11) DEFAULT '1',
  `fri` int(11) DEFAULT '1',
  `satur` int(11) DEFAULT '0',
  `sun` int(11) DEFAULT '0',
  `betweentime` int(255) DEFAULT '0' COMMENT '设置的用户短信接收时间',
  `isactive` int(255) DEFAULT '1' COMMENT '是否接收短信 0不接受 1接收',
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_sms_user_role` (`userid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- ----------------------------
-- Records of sys_sms_userrole
-- ----------------------------
INSERT INTO `sys_sms_userrole` VALUES ('1', '0', '09:00:00', '11:30:00', '14:00:00', '16:30:00', '1', '1', '1', '1', '1', '0', '0', '0', '1');
INSERT INTO `sys_sms_userrole` VALUES ('2', '1', '07:00:00', '12:30:00', '13:00:00', '17:30:00', '1', '1', '1', '1', '1', '0', '1', '0', '1');
INSERT INTO `sys_sms_userrole` VALUES ('3', '2', '09:00:00', '12:30:00', '13:00:00', '16:30:00', '1', '1', '1', '1', '1', '0', '0', '3', '1');
INSERT INTO `sys_sms_userrole` VALUES ('6', '3', '09:00:00', '12:30:00', '13:00:00', '16:30:00', '1', '1', '1', '1', '1', '0', '0', '0', '1');
