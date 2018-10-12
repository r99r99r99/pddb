/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : ytocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2017-04-25 13:26:44
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_halfhour_config
-- ----------------------------
DROP TABLE IF EXISTS `aiot_halfhour_config`;
CREATE TABLE `aiot_halfhour_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `starttime` time DEFAULT NULL,
  `endtime` time DEFAULT NULL,
  `collecttime` time DEFAULT NULL,
  `daynum` int(11) NOT NULL DEFAULT '0' COMMENT '此列用于标记于所计算的日期的时间运算.',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=57 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- ----------------------------
-- Records of aiot_halfhour_config
-- ----------------------------
INSERT INTO `aiot_halfhour_config` VALUES ('9', '00:00:00', '00:30:00', '00:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('10', '00:30:00', '01:00:00', '01:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('11', '01:00:00', '01:30:00', '01:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('12', '01:30:00', '02:00:00', '02:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('13', '02:00:00', '02:30:00', '02:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('14', '02:30:00', '03:00:00', '03:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('15', '03:00:00', '03:30:00', '03:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('16', '03:30:00', '04:00:00', '04:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('17', '04:00:00', '04:30:00', '04:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('18', '04:30:00', '05:00:00', '05:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('19', '05:00:00', '05:30:00', '05:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('20', '05:30:00', '06:00:00', '06:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('21', '06:00:00', '06:30:00', '06:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('22', '06:30:00', '07:00:00', '07:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('23', '07:00:00', '07:30:00', '07:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('24', '07:30:00', '08:00:00', '08:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('25', '08:00:00', '08:30:00', '08:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('26', '08:30:00', '09:00:00', '09:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('27', '09:00:00', '09:30:00', '09:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('28', '09:30:00', '10:00:00', '10:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('29', '10:00:00', '10:30:00', '10:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('30', '10:30:00', '11:00:00', '11:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('31', '11:00:00', '11:30:00', '11:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('32', '11:30:00', '12:00:00', '12:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('33', '12:00:00', '12:30:00', '12:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('34', '12:30:00', '13:00:00', '13:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('35', '13:00:00', '13:30:00', '13:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('36', '13:30:00', '14:00:00', '14:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('37', '14:00:00', '14:30:00', '14:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('38', '14:30:00', '15:00:00', '15:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('39', '15:00:00', '15:30:00', '15:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('40', '15:30:00', '16:00:00', '16:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('41', '16:00:00', '16:30:00', '16:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('42', '16:30:00', '17:00:00', '17:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('43', '17:00:00', '17:30:00', '17:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('44', '17:30:00', '18:00:00', '18:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('45', '18:00:00', '18:30:00', '18:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('46', '18:30:00', '19:00:00', '19:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('47', '19:00:00', '19:30:00', '19:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('48', '19:30:00', '20:00:00', '20:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('49', '20:00:00', '20:30:00', '20:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('50', '20:30:00', '21:00:00', '21:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('51', '21:00:00', '21:30:00', '21:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('52', '21:30:00', '22:00:00', '22:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('53', '22:00:00', '22:30:00', '22:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('54', '22:30:00', '23:00:00', '23:00:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('55', '23:00:00', '23:30:00', '23:30:00', '0');
INSERT INTO `aiot_halfhour_config` VALUES ('56', '23:30:00', '24:00:00', '24:00:00', '0');
