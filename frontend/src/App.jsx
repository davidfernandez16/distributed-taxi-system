import React from 'react';
import Map from './components/Map.jsx';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import Auditoria from './components/Auditoria.jsx';

function App() {
  return (
    <div className="App">
      <Router>
        <h1>
          EasyCab Map Viewer 
          <Link to="/auditoria" style={{marginLeft:'20px'}}>
            <button>Ver Auditoría</button>
          </Link>
        </h1>
        <Routes>
          <Route path="/" element={<Map />} />
          <Route path="/auditoria" element={<Auditoria />} />
        </Routes>
      </Router>
    </div>
  );
}

export default App;
