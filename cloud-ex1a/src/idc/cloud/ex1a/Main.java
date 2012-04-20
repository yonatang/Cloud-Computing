package idc.cloud.ex1a;

import jcurses.system.CharColor;
import jcurses.system.Toolkit;

public class Main {

	public static void main(String[] args) {
		Toolkit.drawRectangle(1, 1, 20, 20, new CharColor(CharColor.BLACK, CharColor.GREEN));
	}
}
