package io.learning.algorithm;

import apple.laf.JRSUIUtils.Tree;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 二叉树/树的遍历
 */
public class TreeTravel {

    public static class TreeNode {

        private int value;
        private TreeNode left;
        private TreeNode right;
    }

    /**
     * 层次遍历
     *
     * @param root
     * @return
     */
    public static List<List<Integer>> levelOrder(TreeNode root) {
        LinkedList<TreeNode> queue = new LinkedList<>();
        List<List<Integer>> resultList = new ArrayList();
        if (root == null) {
            return resultList;
        }
        queue.add(root);
        while (!queue.isEmpty()) {
            int size = queue.size();
            List<Integer> list = new ArrayList();
            while (size > 0) {
                TreeNode node = queue.poll();
                list.add(node.value);
                size--;

                if (node.left != null) {
                    queue.add(node.left);
                }
                if (node.right != null) {
                    queue.add(node.right);
                }
            }
            resultList.add(list);
        }
        return resultList;
    }

    public static void main(String[] args) {

    }
}




