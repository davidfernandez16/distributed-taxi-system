import React, { useEffect, useState, useMemo } from 'react';
import { io } from 'socket.io-client';
import './Map.css';

const Map = () => {
  const [mapData, setMapData] = useState({ taxis: [], clientes: [], locations: [] });

  const gridData = useMemo(() => {
    const grid = Array.from({ length: 20 }, () => Array.from({ length: 20 }, () => ({})));

    mapData.taxis.forEach((taxi) => {
      const { localizacion_x: x, localizacion_y: y, id, availability } = taxi;
      grid[y][x].taxi = { id, availability };
    });

    mapData.clientes.forEach((cliente) => {
      const { localizacion_x: x, localizacion_y: y, id, recogido } = cliente;
      if (!recogido) {
        grid[y][x].cliente = { id };
      }
    });

    mapData.locations.forEach((location) => {
      const { pos_x: x, pos_y: y, id } = location;
      grid[y][x].location = { id };
    });

    return grid;
  }, [mapData]);

  useEffect(() => {
    const socket = io('http://127.0.0.1:5002'); // Cambia la URL si es necesario

    socket.on('update_map', (data) => {
      setMapData(data);
    });

    return () => {
      socket.disconnect();
    };
  }, []);

  return (
    <div className="map-container">
      <div className="grid">
        {gridData.map((row, rowIndex) => (
          <div key={rowIndex} className="row">
            {row.map((cell, colIndex) => (
              <div key={colIndex} className="cell">
                {cell.taxi && (
                  <div
                    className="entity taxi"
                    style={{
                      backgroundColor: cell.taxi.availability === 'AVAILABLE' ? 'green' : 'red',
                    }}
                  >
                    T{cell.taxi.id}
                  </div>
                )}
                {cell.cliente && <div className="entity cliente">C{cell.cliente.id}</div>}
                {cell.location && <div className="entity location">{cell.location.id}</div>}
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  );
};

export default Map;
