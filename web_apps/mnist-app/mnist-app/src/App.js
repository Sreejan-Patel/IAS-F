import logo from './logo.svg';
import './App.css';

import MNISTDigitRecognition from './digit_commit';
import ParentComponent from './parentComponent';

function App() {
  return (
    <div className="App">
      <MNISTDigitRecognition />
      <ParentComponent />
    </div>
  );
}

export default App;
