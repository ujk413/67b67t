import argon2 from 'argon2';

const hash = '$argon2id$v=19$m=65536,t=3,p=4$IN9m/Ral2y3hAAE9wNwm+g$+zJZ1LN4QUp3L3tnXtF2ST+t5MvWHHNLTKYif0PBN/U';
const password = 'Sad99-rom-';

console.log('Hash:', hash);
console.log('Password:', password);

argon2.verify(hash, password)
  .then(isValid => {
    console.log('Password valid:', isValid);
  })
  .catch(err => {
    console.error('Error:', err);
  });
