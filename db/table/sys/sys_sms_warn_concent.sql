/*
Navicat MySQL Data Transfer

Source Server         : localhost_3306
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : sdocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2016-06-22 11:12:15
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for sys_sms_warn_concent
-- ----------------------------
DROP TABLE IF EXISTS `sys_sms_warn_concent`;
CREATE TABLE `sys_sms_warn_concent` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stationid` int(11) DEFAULT NULL,
  `collect_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `type` int(11) DEFAULT '0' COMMENT '短信类型 sys_public 0010',
  `ifsend` int(11) DEFAULT '0' COMMENT '是否生成短信:0:未生成 1:已生成 2:站点不允许发送短信',
  `concent` varchar(2550) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=725 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
