// Keep issuance validation, message insertion and single-use dice binding in
// one transaction. HTTP, WebSocket and channel comments share this boundary.
export function createMessageInserter(pool,getFeatures){
 return async(sql,values,context)=>{
  const features=getFeatures();
  const client=await pool.connect();
  try{
   await client.query('BEGIN');
   await features.validatePlaintext(context);
   const result=await client.query(sql,values);
   if(result.rows[0])await features.bindDice({...context,messageId:result.rows[0].id,query:client.query.bind(client)});
   await client.query('COMMIT');return result;
  }catch(error){await client.query('ROLLBACK');throw error;}
  finally{client.release();}
 };
}
