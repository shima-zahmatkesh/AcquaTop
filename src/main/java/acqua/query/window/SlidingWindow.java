package acqua.query.window;

import java.util.Iterator;


public abstract class SlidingWindow implements Iterator<Window> {
	protected int slide, width;
	protected Window currentWindow;
	protected long currentTimestamp;
	
	public SlidingWindow(int width, int slide, long initialTimestamp){
		this.slide=slide;
		this.width=width;
		this.currentTimestamp = initialTimestamp+width;
	}
	
	public Window next() {
		if(currentWindow==null){
			currentWindow = new Window(currentTimestamp-width, currentTimestamp , 1 );
			return currentWindow;
		}
		currentTimestamp += slide;
		Window nextWindow = new Window(currentTimestamp-width, currentTimestamp, currentWindow , currentWindow.id+1 );
		currentWindow = nextWindow;
		return currentWindow;
	}

	public void remove() {
		throw new RuntimeException("Not implemented");
	}
	


}
