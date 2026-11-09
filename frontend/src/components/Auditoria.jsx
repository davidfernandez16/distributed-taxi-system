import React, { useEffect, useState } from 'react';

function Auditoria() {
  const [events, setEvents] = useState([]);

  useEffect(() => {
    fetch('/api/auditoria')
      .then(response => response.json())
      .then(data => setEvents(data))
      .catch(err => console.error('Error fetching auditoria:', err));
  }, []);

  return (
    <div>
      <h2>Auditoría de Eventos</h2>
      <table border="1">
        <thead>
          <tr>
            <th>Fecha/Hora</th>
            <th>IP Origen</th>
            <th>Acción</th>
            <th>Descripción</th>
          </tr>
        </thead>
        <tbody>
          {events.map((evt, index) => (
            <tr key={index}>
              <td>{evt.fecha_hora}</td>
              <td>{evt.ip_origen}</td>
              <td>{evt.accion}</td>
              <td>{evt.descripcion}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default Auditoria;
