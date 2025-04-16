import React, { useState } from 'react';

function App() {
  const [todos, setTodos] = useState([]);
  const [inputValue, setInputValue] = useState('');

  const handleAddTodo = () => {
    if (inputValue.trim() !== '') {
      setTodos([...todos, { id: Date.now(), text: inputValue, completed: false }]);
      setInputValue('');
    }
  };

  const handleToggleTodo = (id) => {
    setTodos(todos.map(todo => 
      todo.id === id ? { ...todo, completed: !todo.completed } : todo
    ));
  };

  const handleDeleteTodo = (id) => {
    setTodos(todos.filter(todo => todo.id !== id));
  };

  return (
    <div style={{ maxWidth: '500px', margin: '0 auto', padding: '20px' }}>
      <h1>DBMS Todo App</h1>
      
      <div style={{ display: 'flex', marginBottom: '20px' }}>
        <input
          type="text"
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          onKeyPress={(e) => e.key === 'Enter' && handleAddTodo()}
          placeholder="Add a new todo"
          style={{ flex: 1, padding: '8px', marginRight: '10px' }}
        />
        <button 
          onClick={handleAddTodo}
          style={{ padding: '8px 16px' }}
        >
          Add
        </button>
      </div>

      <ul style={{ listStyleType: 'none', padding: 0 }}>
        {todos.map(todo => (
          <li 
            key={todo.id} 
            style={{ 
              display: 'flex', 
              alignItems: 'center', 
              padding: '10px',
              marginBottom: '5px',
              backgroundColor: '#f5f5f5',
              borderRadius: '4px',
              textDecoration: todo.completed ? 'line-through' : 'none'
            }}
          >
            <input
              type="checkbox"
              checked={todo.completed}
              onChange={() => handleToggleTodo(todo.id)}
              style={{ marginRight: '10px' }}
            />
            <span style={{ flex: 1 }}>{todo.text}</span>
            <button 
              onClick={() => handleDeleteTodo(todo.id)}
              style={{ 
                border: 'none', 
                background: 'transparent', 
                color: 'red',
                cursor: 'pointer' 
              }}
            >
              ✕
            </button>
          </li>
        ))}
      </ul>
      
      {todos.length === 0 && (
        <p style={{ textAlign: 'center', color: '#888' }}>
          No todos yet! Add one above.
        </p>
      )}
    </div>
  );
}

export default App;
