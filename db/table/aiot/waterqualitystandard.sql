/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : syocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2016-10-28 14:43:45
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for waterqualitystandard
-- ----------------------------
DROP TABLE IF EXISTS `waterqualitystandard`;
CREATE TABLE `waterqualitystandard` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `item` varchar(32) CHARACTER SET utf8 DEFAULT NULL COMMENT '项目名称',
  `standard_grade` int(11) NOT NULL COMMENT '分类名称（Ⅰ类	Ⅱ类	Ⅲ类	Ⅳ类	Ⅴ类）',
  `min_value` double(32,4) DEFAULT NULL COMMENT '最小值',
  `max_value` double(32,4) DEFAULT NULL COMMENT '最大值',
  `remarks` varchar(32) CHARACTER SET utf8 DEFAULT NULL COMMENT '说明',
  `water_type` int(11) DEFAULT '1' COMMENT '水质标准类型1:海洋水质,2:地表水质',
  `min` int(11) NOT NULL DEFAULT '0',
  `max` int(11) NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`),
  UNIQUE KEY `i_query` (`item`,`standard_grade`,`min_value`,`water_type`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=39 DEFAULT CHARSET=latin1;

-- ----------------------------
-- Records of waterqualitystandard
-- ----------------------------
INSERT INTO `waterqualitystandard` VALUES ('18', 'DO', '1', '6.0000', '100.0000', null, '2', '0', '1');
INSERT INTO `waterqualitystandard` VALUES ('19', 'DO', '2', '5.0000', '6.0000', '备注3', '2', '0', '1');
INSERT INTO `waterqualitystandard` VALUES ('20', 'DO', '3', '4.0000', '5.0000', null, '2', '0', '1');
INSERT INTO `waterqualitystandard` VALUES ('21', 'DO', '4', '0.0000', '4.0000', null, '2', '0', '1');
INSERT INTO `waterqualitystandard` VALUES ('33', 'pH', '1', '7.8000', '8.5000', null, '2', '0', '1');
INSERT INTO `waterqualitystandard` VALUES ('36', 'pH', '3', '0.0000', '7.8000', null, '2', '0', '1');
INSERT INTO `waterqualitystandard` VALUES ('37', 'pH', '3', '8.5000', '10.0000', null, '2', '0', '1');
