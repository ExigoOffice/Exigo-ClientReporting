using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace Exigo.ClientReporting
{
    public class Tree
    {
        public bool SortByPlacement { get; set; }

        Node _rootNode;
        NodeKeyedList _allNodes;
        //internal DataCache _cache;

        //internal Tree(DataCache cache)
        internal Tree()
        {
            //_cache = cache;
            _rootNode = new Node();// Node.Pool.GetObject();
            _allNodes = new NodeKeyedList();
            _allNodes.Add(_rootNode);
        }

        //internal DataCache Cache
        //{
        //    get { return _cache; }
        //}

        public Node RootNode
        {
            get { return _rootNode; }
        }

        public IReadOnlyKeyedList<int, Node> AllNodes
        {
            get { return _allNodes; }
        }

        public NodeList Nodes
        {
            get { return _rootNode.Nodes; }
        }

        internal void InsertNode(Node node, Node parent)
        {
            node._tree = this;

            //we know who our parent is
            node._parent = parent;

            //are we the first child?
            //add to the first position
            if (parent._firstChild == null)
            {
                parent._firstChild = node;
                parent._lastChild = node;
            }
            //If we are sorting
            else if (SortByPlacement)
            {
                //if our position is less then the last position then we need to see where to insert ourselves, otherwise insert at end
                if (node.Placement < parent._lastChild.Placement)
                {

                    //if we are not the first child then we need to mark the last sibling to us (linked list)
                    Node insertBefore = parent._firstChild;
                    Node insertAfter = null;
                    while (node.Placement > insertBefore.Placement)
                    {
                        insertAfter = insertBefore;
                        insertBefore = insertBefore._firstSibling;
                    }

                    node._firstSibling = insertBefore;

                    //we might be inserting in the middle
                    if (insertAfter != null)
                        insertAfter._firstSibling = node;

                    //we may be taking the first spot
                    if (parent._firstChild == insertBefore)
                        parent._firstChild = node;

                }
                //for a very large amount of siblings, this will save time, test once and insert at the end
                else
                {
                    parent._lastChild._firstSibling = node;
                    parent._lastChild = node;
                }
            }
            //If we are not sorting, then simply add to the end
            else
            {
                parent._lastChild._firstSibling = node;
                parent._lastChild = node;
            }

            _allNodes.Add(node);
        }

        /// <summary>
        /// Clears all nodes and returns them to the node pool.
        /// </summary>
        internal void Clear()
        {
            _allNodes.Clear();
            //_rootNode = Node.Pool.GetObject();
            _rootNode = new Node();
            _allNodes.Add(_rootNode);
            Modified = DateTime.MinValue;
        }

        public DateTime Modified { get; set; }

        //used when building from xml
        internal void BuildLftRgt()
        {
            int index = 1;
            RootNode.BuildLftRgt(ref index, 0);
        }
    }

    public class Node
    {
        #region Pool Factory
        //private static Pool<Node> _pool = new Pool<Node>(
        //    () => new Node(),
        //    Node.Reset);
        //internal static Pool<Node> Pool { get { return _pool; } }
        //internal static void Reset(Node o)
        //{
        //    o._parent = null;
        //    o._firstSibling = null;
        //    o._firstChild = null;
        //    o._lastChild = null;
        //    o._tree = null;
        //    o._nodeID = 0;
        //    o._parentID = 0;
        //    o._customerID = 0;
        //    o._nestedLevel = 0;
        //    o._placement = 0;
        //    o._lft = 0;
        //    o._rgt = 0;
        //    o._instance = 0;
        //    o._customer = null;
        //}

        public Node()
        {
            _nodes = new NodeList(this);

        }

        #endregion
        //this doesn't need to be pooled as it will stay with this object....
        NodeList _nodes;

        internal Node _parent;
        internal Node _firstSibling;
        internal Node _firstChild;
        internal Node _lastChild;
        internal Tree _tree;
        int _nodeID;
        int _parentID;
        int _customerID;
        int _nestedLevel;
        int _placement;
        int _lft;
        int _rgt;
        int _instance;

        internal Customer _customer;

        public Node Parent
        {
            get { return _parent; }
        }

        public Customer Customer
        {
            get
            {
                return _customer;
            }
        }


        public int NodeID
        {
            get { return _nodeID; }
            internal set { _nodeID = value; }
        }

        public override string ToString()
        {
            return "NodeID: " + NodeID + ", ParentID: " + ParentID;
        }

        public int ParentID
        {
            get { return _parentID; }
            internal set { _parentID = value; }
        }

        public int CustomerID
        {
            get { return _customerID; }
            internal set { _customerID = value; }
        }

        public int NestedLevel
        {
            get { return _nestedLevel; }
            internal set { _nestedLevel = value; }
        }

        public int Placement
        {
            get { return _placement; }
            internal set { _placement = value; }
        }

        public int Instance
        {
            get { return _instance; }
            internal set { _instance = value; }
        }

        public int Lft
        {
            get { return _lft; }
            internal set { _lft = value; }
        }

        public int Rgt
        {
            get { return _rgt; }
            internal set { _rgt = value; }
        }

        public NodeList Nodes
        {
            get { return _nodes; }
        }

        public Node[] GetAllChildNodes(int maxDepth, bool sortByLevel)
        {
            return AllChildNodes(maxDepth, sortByLevel).ToArray();
        }

        public Node[] GetAllChildNodes()
        {
            return AllChildNodes(999999999, true).ToArray();
        }

        public IEnumerable<Node> GetAllChildNodes(int maxDepth)
        {
            return AllChildNodes(maxDepth, true);
        }

        public IEnumerable<Node> AllChildNodes()
        {
            return AllChildNodes(999999999, true);
        }

        public IEnumerable<Node> AllChildNodes(int maxDepth, bool sortByLevel)
        {
            //a queue represents all nodes on one level
            Queue<Node> parentQueue = new Queue<Node>(50);
            Queue<Node> childQueue = new Queue<Node>(50);

            int currentDepth = 0;

            //we'll start with the root node
            parentQueue.Enqueue(this);

            while (parentQueue.Count > 0)
            {
                while (parentQueue.Count > 0)
                {
                    Node parentNode = parentQueue.Dequeue();

                    for (Node childNode = parentNode._firstChild; childNode != null; childNode = childNode._firstSibling)
                    {
                        yield return childNode;

                        //put the child node into a child queue so we can scan this level in a sec
                        childQueue.Enqueue(childNode);
                    }
                }

                //TODO: see if we really mean 1 or 2 levels
                currentDepth++;
                if (currentDepth >= maxDepth)
                    yield break;

                //the parent queue is cleared out by this time so switch it
                var holdChild = childQueue;
                var holdParent = parentQueue;
                parentQueue = holdChild;
                childQueue = holdParent; //this is now blank and ready to be the new child
            }
        }

        internal void BuildLftRgt(ref int index, int nestedLevel)
        {
            _nestedLevel = nestedLevel;

            _lft = index;
            index++;

            bool hasIncremented = false;
            foreach (Node node in Nodes)
            {

                if (!hasIncremented)
                {
                    nestedLevel++;
                    hasIncremented = true;
                }
                node.BuildLftRgt(ref index, nestedLevel);
            }
            _rgt = index;
            index++;
        }

        public IEnumerable<Node> OLDAllChildNodes(int maxDepth, bool sortByLevel)
        {
            //Note: this currenty returns it all always sorted by level
            //we could write another that returns it non sorted if necessary

            int currentDepth = 0;
            Node currentParent = this;
            Node nextDown = null;
            Node childNode = null;
            while (currentDepth < maxDepth && currentParent != null)
            {
                //nextDown = currentParent._firstChild;
                //we don't know which is the next down
                nextDown = null;
                while (currentParent != null)
                {
                    for (childNode = currentParent._firstChild; childNode != null; childNode = childNode._firstSibling)
                    {
                        //this one could be the next path to scan down (it has to have children
                        if (nextDown == null && childNode._firstChild != null)
                            nextDown = childNode;

                        yield return childNode;
                    }
                    //grab the parents sibling unless we are on first level
                    if (currentDepth == 0)
                        currentParent = null;
                    else
                        currentParent = currentParent._firstSibling;
                }
                currentParent = nextDown;
                //now before I let this go
                currentDepth++;
            }
        }
    }

    public class NodeKeyedList : KeyedCollection<int, Node>, IReadOnlyKeyedList<int, Node>
    {
        protected override int GetKeyForItem(Node item)
        {
            return item.NodeID;
        }

        protected override void ClearItems()
        {
            //foreach (var item in this)
            //{
            //    Node.Pool.ReturnObject(item);
            //}
            base.ClearItems();
        }

        public bool ContainsKey(int key)
        {
            return base.Contains(key);
        }




        #region IReadOnlyKeyedList<int,Node> Members


        public bool TryGetValue(int key, out Node item)
        {
            return Dictionary.TryGetValue(key, out item);
        }

        #endregion
    }


    public class NodeList : IEnumerable<Node>
    {
        Node _parent;
        internal NodeList(Node parent)
        {
            _parent = parent;
        }
        #region IReadOnlyList<Node> Members

        public int Count
        {
            get
            {
                int count = 0;
                for (Node p = _parent._firstChild; p != null; p = p._firstSibling)
                {
                    count++;
                }
                return count;
            }
        }

        public Node this[int index]
        {
            get
            {
                int i = 0;
                for (Node node = _parent._firstChild; node != null; node = node._firstSibling)
                {
                    if (i == index)
                        return node;

                    i++;
                }
                throw new IndexOutOfRangeException();
            }
        }

        #endregion

        #region IEnumerable<Node> Members

        IEnumerator<Node> IEnumerable<Node>.GetEnumerator()
        {
            for (Node node = _parent._firstChild; node != null; node = node._firstSibling)
            {
                yield return node;
            }
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<Node>)this).GetEnumerator();
        }

        #endregion
    }
}
