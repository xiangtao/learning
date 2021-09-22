package learning.juc;

import java.util.Arrays;

public class T {

    static int[] arr;

    private static void swap(int x, int y) {
        int temp = arr[x];
        arr[x] = arr[y];
        arr[y] = temp;

    }

    private static void quick_sort_recursive(int start, int end) {
        if (start >= end) {
            return;
        }
        int mid = arr[end];
        int left = start, right = end - 1;
        while (left < right) {
            while (arr[left] < mid && left < right) {
                left++;
            }
            while (arr[right] >= mid && left < right) {
                right--;
            }
            swap(left, right);
        }
        if (arr[left] >= arr[end]) {
            swap(left, end);
        } else {
            left++;
        }
        quick_sort_recursive(start, left - 1);
        quick_sort_recursive(left + 1, end);
    }

    public static void sort(int[] arrin) {
        arr = arrin;
        quick_sort_recursive(0, arr.length - 1);

    }

    public static void main(String args[]) {
        int[] a = {354, 656, 21, 2342, 65, 657, 76, 12, 54, 778, 50, 31, 333, 45, 56, 86, 97, 121};
        sort(a);
        System.out.print(Arrays.toString(arr));

    }
}
