/*
 Navicat Premium Data Transfer

 Source Server         : node-1
 Source Server Type    : MySQL
 Source Server Version : 50731
 Source Host           : node-1.51doit.cn:3306
 Source Schema         : doit31

 Target Server Type    : MySQL
 Target Server Version : 50731
 File Encoding         : 65001

 Date: 09/06/2022 17:56:47
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for tb_wordcount
-- ----------------------------
DROP TABLE IF EXISTS `tb_wordcount`;
CREATE TABLE `tb_wordcount` (
  `word` varchar(255) NOT NULL,
  `counts` int(11) DEFAULT NULL,
  PRIMARY KEY (`word`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

SET FOREIGN_KEY_CHECKS = 1;
