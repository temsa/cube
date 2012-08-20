function createTree(root){ 
  if(!root) return root;
  Object.keys(root).forEach(function(el){
  var value = root[el];
  var splitted = el.split('.')

  var object=root;
  var last = root;
  for(var i=0 ; i<splitted.length ;i++){
    object[splitted[i]] = object[splitted[i]] || {};
    last = object;
    object = object[splitted[i]];
  };
  last[splitted[splitted.length-1]] =value; 
});
 return root;}

exports.createTree = createTree;
