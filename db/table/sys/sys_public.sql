/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 80011
Source Host           : localhost:3306
Source Database       : hnocean

Target Server Type    : MYSQL
Target Server Version : 80011
File Encoding         : 65001

Date: 2018-08-07 10:15:59
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for sys_public
-- ----------------------------
DROP TABLE IF EXISTS `sys_public`;
CREATE TABLE `sys_public` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `parentcode` varchar(20) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `classid` int(11) DEFAULT NULL,
  `classcode` varchar(20) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `classname` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `value` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `ordercode` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '排序',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=108 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- ----------------------------
-- Records of sys_public
-- ----------------------------
INSERT INTO `sys_public` VALUES ('7', '0001', '1', '1', 'userlogin', '用户名登录', null, null);
INSERT INTO `sys_public` VALUES ('8', '0001', '2', '2', 'calogin', 'CA登录', null, null);
INSERT INTO `sys_public` VALUES ('9', '0002', '1', '1', 'sdocean', '元系统', null, null);
INSERT INTO `sys_public` VALUES ('10', '0003', '1', '1', 'menurole', '菜单权限', null, '001');
INSERT INTO `sys_public` VALUES ('11', '0003', '2', '2', 'stationrole', '站点权限', null, '002');
INSERT INTO `sys_public` VALUES ('12', '0004', '1', '1', 'true', '启用', null, null);
INSERT INTO `sys_public` VALUES ('13', '0004', '0', '0', 'false', '禁用', null, null);
INSERT INTO `sys_public` VALUES ('14', '0005', '1', '1', 'jinhai', '淡水水质', null, null);
INSERT INTO `sys_public` VALUES ('15', '0005', '2', '2', 'shenhai', '海水水质', null, null);
INSERT INTO `sys_public` VALUES ('16', '0006', '1', 'ceshi', '测试', 'lava', '备注', '0001');
INSERT INTO `sys_public` VALUES ('23', '0008', '1', '1', 'warn', '告警信息', null, '001');
INSERT INTO `sys_public` VALUES ('24', '0008', '2', '2', 'alarm', '预警信息', null, '002');
INSERT INTO `sys_public` VALUES ('25', '0009', '1', '1', 'day', '日', null, '001');
INSERT INTO `sys_public` VALUES ('27', '0009', '3', '3', 'month', '月', null, '003');
INSERT INTO `sys_public` VALUES ('28', '0010', '1', '1', 'sms', '参数预警', '短信模板', '001');
INSERT INTO `sys_public` VALUES ('29', '0010', '2', '2', 'sms', '系统预警', '短信模板', '002');
INSERT INTO `sys_public` VALUES ('30', '0011', '1', '1', 'ADD', '新增', null, '001');
INSERT INTO `sys_public` VALUES ('31', '0011', '2', '2', 'UPDATE', '修改', null, '002');
INSERT INTO `sys_public` VALUES ('34', '0012', '2', '2', 'change', '站点切换', null, '002');
INSERT INTO `sys_public` VALUES ('35', '0003', '3', '3', 'firstmenuRole', '首页角色', null, '003');
INSERT INTO `sys_public` VALUES ('36', '0013', '0', '0', 'nop', '基础数据', null, '001');
INSERT INTO `sys_public` VALUES ('37', '0013', '1', '1', 'pollu', '入海污染量数据', null, '002');
INSERT INTO `sys_public` VALUES ('38', '0014', '1', '1', 'mainplan', '预计维护', null, '001');
INSERT INTO `sys_public` VALUES ('39', '0014', '2', '2', 'maining', '正在维护', null, '002');
INSERT INTO `sys_public` VALUES ('40', '0014', '3', '3', 'mainend', '维护结束', null, '002');
INSERT INTO `sys_public` VALUES ('41', '0015', '1', '1', 'install', '安装设备', null, '001');
INSERT INTO `sys_public` VALUES ('42', '0015', '2', '2', 'planmain', '例行维护', null, '002');
INSERT INTO `sys_public` VALUES ('43', '0015', '3', '3', 'errormain', '突发状况', null, '003');
INSERT INTO `sys_public` VALUES ('44', '0015', '4', '4', 'uninistall', '卸载设备', null, '004');
INSERT INTO `sys_public` VALUES ('45', '0016', '1', '1', 'report', '上报', null, '001');
INSERT INTO `sys_public` VALUES ('46', '0016', '2', '2', 'reason', '分析', null, '002');
INSERT INTO `sys_public` VALUES ('47', '0016', '3', '3', 'do', '修错', null, '003');
INSERT INTO `sys_public` VALUES ('48', '0016', '4', '4', 'finish', '完成', null, '004');
INSERT INTO `sys_public` VALUES ('51', '0017', '1', '1', 'hour', '小时', null, '001');
INSERT INTO `sys_public` VALUES ('61', '0017', '2', '2', 'day', '天', null, '002');
INSERT INTO `sys_public` VALUES ('71', '0017', '3', '3', 'week', '周', null, '003');
INSERT INTO `sys_public` VALUES ('81', '0017', '4', '4', 'month', '月', null, '004');
INSERT INTO `sys_public` VALUES ('91', '0017', '5', '5', 'year', '年', null, '005');
INSERT INTO `sys_public` VALUES ('101', '0003', '4', '4', 'gps', '站点GPS权限', null, '004');
INSERT INTO `sys_public` VALUES ('102', '0019', '1', '1', 'greaterequal', '>=', null, '001');
INSERT INTO `sys_public` VALUES ('103', '0019', '2', '2', 'greater', '>', null, '002');
INSERT INTO `sys_public` VALUES ('104', '0019', '3', '3', 'equal', '=', null, '003');
INSERT INTO `sys_public` VALUES ('105', '0019', '4', '4', 'noequal', '!=', null, '004');
INSERT INTO `sys_public` VALUES ('106', '0019', '5', '5', 'smaller', '<', null, '005');
INSERT INTO `sys_public` VALUES ('107', '0019', '6', '6', 'smallerequal', '<=', null, '006');
